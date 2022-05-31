/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"

#include <errno.h>
#include <fstream>
#include <fuse/fuse.h>

#include "common/crc.h"
#include "common/md5.h"
#include "protocol/MFSCommunication.h"
#include "common/mfserr.h"
#include "common/sockets.h"
#include "mount/fuse/mfs_fuse.h"
#include "mount/fuse/mfs_meta_fuse.h"
#include "mount/fuse/mount_config.h"
#include "mount/g_io_limiters.h"
#include "mount/mastercomm.h"
#include "mount/masterproxy.h"
#include "mount/readdata.h"
#include "mount/stats.h"
#include "mount/symlinkcache.h"
#include "mount/writedata.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)

static void mfs_fsinit (void *userdata, struct fuse_conn_info *conn);

static struct fuse_lowlevel_ops mfs_meta_oper;

static struct fuse_lowlevel_ops mfs_oper;

static void init_fuse_lowlevel_ops() {
	mfs_meta_oper.init = mfs_fsinit;
	mfs_meta_oper.statfs = mfs_meta_statfs;
	mfs_meta_oper.lookup = mfs_meta_lookup;
	mfs_meta_oper.getattr = mfs_meta_getattr;
	mfs_meta_oper.setattr = mfs_meta_setattr;
	mfs_meta_oper.unlink = mfs_meta_unlink;
	mfs_meta_oper.rename = mfs_meta_rename;
	mfs_meta_oper.opendir = mfs_meta_opendir;
	mfs_meta_oper.readdir = mfs_meta_readdir;
	mfs_meta_oper.releasedir = mfs_meta_releasedir;
	mfs_meta_oper.open = mfs_meta_open;
	mfs_meta_oper.release = mfs_meta_release;
	mfs_meta_oper.read = mfs_meta_read;
	mfs_meta_oper.write = mfs_meta_write;

	mfs_oper.init = mfs_fsinit;
	mfs_oper.statfs = mfs_statfs;
	mfs_oper.lookup = mfs_lookup;
	mfs_oper.getattr = mfs_getattr;
	mfs_oper.setattr = mfs_setattr;
	mfs_oper.mknod = mfs_mknod;
	mfs_oper.unlink = mfs_unlink;
	mfs_oper.mkdir = mfs_mkdir;
	mfs_oper.rmdir = mfs_rmdir;
	mfs_oper.symlink = mfs_symlink;
	mfs_oper.readlink = mfs_readlink;
	mfs_oper.rename = mfs_rename;
	mfs_oper.link = mfs_link;
	mfs_oper.opendir = mfs_opendir;
	mfs_oper.readdir = mfs_readdir;
	mfs_oper.releasedir = mfs_releasedir;
	mfs_oper.create = mfs_create;
	mfs_oper.open = mfs_open;
	mfs_oper.release = mfs_release;
	mfs_oper.flush = mfs_flush;
	mfs_oper.fsync = mfs_fsync;
	mfs_oper.read = mfs_read;
	mfs_oper.write = mfs_write;
	mfs_oper.access = mfs_access;
	mfs_oper.getxattr = mfs_getxattr;
	mfs_oper.setxattr = mfs_setxattr;
	mfs_oper.listxattr = mfs_listxattr;
	mfs_oper.removexattr = mfs_removexattr;
#if FUSE_VERSION >= 26
	if (gMountOptions.filelocks) {
		mfs_oper.getlk = lzfs_getlk;
		mfs_oper.setlk = lzfs_setlk;
	}
#endif
#if FUSE_VERSION >= 29
	if (gMountOptions.filelocks) {
		mfs_oper.flock = lzfs_flock;
	}
#endif
}

static void mfs_fsinit (void *userdata, struct fuse_conn_info *conn) {
#if (FUSE_VERSION >= 28)
	conn->want |= FUSE_CAP_DONT_MASK;
#else
		(void)conn;
#endif

	int *piped = (int*)userdata;
	if (piped[1]>=0) {
		char s = 0;
		if (write(piped[1],&s,1)!=1) {
			lzfs_pretty_syslog(LOG_ERR,"pipe write error: %s",strerr(errno));
		}
		close(piped[1]);
	}
}

int mainloop(struct fuse_args *args,const char* mp,int mt,int fg) {
	struct fuse_session *se;
	struct fuse_chan *ch;
	struct rlimit rls;
	int piped[2];
	char s;
	int err;
	int i;
	md5ctx ctx;
	std::vector<uint8_t> md5pass;

	if (gMountOptions.passwordask && gMountOptions.password==NULL
			&& gMountOptions.md5pass==NULL) {
		gMountOptions.password = getpass("MFS Password:");
	}
	if (gMountOptions.password) {
		md5pass.resize(16);
		md5_init(&ctx);
		md5_update(&ctx,(uint8_t*)(gMountOptions.password),
				strlen(gMountOptions.password));
		md5_final(md5pass.data(),&ctx);
		memset(gMountOptions.password,0,strlen(gMountOptions.password));
	} else if (gMountOptions.md5pass) {
		int ret = md5_parse(md5pass, gMountOptions.md5pass);
		if (ret < 0) {
			fprintf(stderr,"bad md5 definition (md5 should be given as 32 hex digits)\n");
			return 1;
		}
		memset(gMountOptions.md5pass,0,strlen(gMountOptions.md5pass));
	}

	if (fg==0) {
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY , LOG_DAEMON);
	} else {
#if defined(LOG_PERROR)
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY | LOG_PERROR, LOG_USER);
#else
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY, LOG_USER);
#endif
	}
	lzfs::add_log_syslog();

	rls.rlim_cur = gMountOptions.nofile;
	rls.rlim_max = gMountOptions.nofile;
	setrlimit(RLIMIT_NOFILE,&rls);

	setpriority(PRIO_PROCESS,getpid(),gMountOptions.nice);
#ifdef MFS_USE_MEMLOCK
	if (gMountOptions.memlock) {
		rls.rlim_cur = RLIM_INFINITY;
		rls.rlim_max = RLIM_INFINITY;
		if (setrlimit(RLIMIT_MEMLOCK,&rls)<0) {
			gMountOptions.memlock=0;
		}
	}
#endif

	piped[0] = piped[1] = -1;
	if (fg==0) {
		if (pipe(piped)<0) {
			fprintf(stderr,"pipe error\n");
			return 1;
		}
		err = fork();
		if (err<0) {
			fprintf(stderr,"fork error\n");
			return 1;
		} else if (err > 0) {
			std::fill(md5pass.begin(), md5pass.end(), 0);
			close(piped[1]);
			err = read(piped[0],&s,1);
			if (err==0) {
				s=1;
			}
			return s;
		}
		close(piped[0]);
		s=1;
	} else {
		lzfs::add_log_stderr(lzfs::log_level::debug);
	}


#ifdef MFS_USE_MEMLOCK
	if (gMountOptions.memlock) {
		if (mlockall(MCL_CURRENT|MCL_FUTURE)==0) {
			lzfs_pretty_syslog(LOG_NOTICE,"process memory was successfully locked in RAM");
		}
	}
#endif

	LizardClient::FsInitParams params(
		gMountOptions.bindhost ? gMountOptions.bindhost : "",
		gMountOptions.masterhost, gMountOptions.masterport, mp);
	params.verbose = true;
	params.meta = gMountOptions.meta;
	params.subfolder = gMountOptions.subfolder;
	params.password_digest = std::move(md5pass);
	params.do_not_remember_password = gMountOptions.donotrememberpassword;
	params.delayed_init = gMountOptions.delayedinit;
	params.report_reserved_period = gMountOptions.reportreservedperiod;
	params.io_retries = gMountOptions.ioretries;
	params.io_limits_config_file = gMountOptions.iolimits ? gMountOptions.iolimits : "";
	params.bandwidth_overuse = gMountOptions.bandwidthoveruse;

	params.chunkserver_round_time_ms = gMountOptions.chunkserverrtt;
	params.chunkserver_connect_timeout_ms = gMountOptions.chunkserverconnectreadto;
	params.chunkserver_wave_read_timeout_ms = gMountOptions.chunkserverwavereadto;
	params.total_read_timeout_ms = gMountOptions.chunkservertotalreadto;
	params.cache_expiration_time_ms = gMountOptions.cacheexpirationtime;
	params.readahead_max_window_size_kB = gMountOptions.readaheadmaxwindowsize;
	params.prefetch_xor_stripes = gMountOptions.prefetchxorstripes;
	params.bandwidth_overuse = gMountOptions.bandwidthoveruse;
	params.write_cache_size = gMountOptions.writecachesize;
	params.write_workers = gMountOptions.writeworkers;
	params.write_window_size = gMountOptions.writewindowsize;
	params.chunkserver_write_timeout_ms = gMountOptions.chunkserverwriteto;
	params.cache_per_inode_percentage = gMountOptions.cachePerInodePercentage;

	params.keep_cache = gMountOptions.keepcache;
	params.direntry_cache_timeout = gMountOptions.direntrycacheto;
	params.direntry_cache_size = gMountOptions.direntrycachesize;
	params.entry_cache_timeout = gMountOptions.entrycacheto;
	params.attr_cache_timeout = gMountOptions.attrcacheto;
	params.mkdir_copy_sgid = gMountOptions.mkdircopysgid;
	params.sugid_clear_mode = gMountOptions.sugidclearmode;
	params.use_rw_lock = gMountOptions.rwlock;
	params.acl_cache_timeout = gMountOptions.aclcacheto;
	params.acl_cache_size = gMountOptions.aclcachesize;

	params.debug_mode = gMountOptions.debug;
	if (gMountOptions.meta == 0) {
		try {
			LizardClient::fs_init(params);
		} catch (...) {
			if (piped[1] >= 0) {
				if (write(piped[1], &s, 1) != 1) {
					fprintf(stderr,"pipe write error\n");
				}
				close(piped[1]);
			}
			return 1;
		}
	} else {
		masterproxy_init();
		symlink_cache_init();
		if (gMountOptions.delayedinit) {
			fs_init_master_connection(params);
		} else {
			if (fs_init_master_connection(params) < 0) {
				if (piped[1] >= 0) {
					if (write(piped[1], &s, 1) != 1) {
						fprintf(stderr,"pipe write error\n");
					}
					close(piped[1]);
				}
				return 1;
			}
		}
		fs_init_threads(params.io_retries);
	}

	ch = fuse_mount(mp, args);
	if (ch == NULL) {
		fprintf(stderr,"error in fuse_mount\n");
		if (piped[1]>=0) {
			if (write(piped[1], &s, 1) != 1) {
				fprintf(stderr,"pipe write error\n");
			}
			close(piped[1]);
		}
		if (gMountOptions.meta == 0) {
			LizardClient::fs_term();
		} else {
			masterproxy_term();
			fs_term();
			symlink_cache_term();
		}
		return 1;
	}

	if (gMountOptions.meta) {
		mfs_meta_init(gMountOptions.debug,
				gMountOptions.entrycacheto,
				gMountOptions.attrcacheto);
		se = fuse_lowlevel_new(args, &mfs_meta_oper, sizeof(mfs_meta_oper), (void*)piped);
	} else {
		se = fuse_lowlevel_new(args, &mfs_oper, sizeof(mfs_oper), (void*)piped);
	}
	if (se == NULL) {
		fuse_unmount(mp,ch);
		fprintf(stderr,"error in fuse_lowlevel_new\n");
		usleep(100000); // time for print other error messages by FUSE
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				fprintf(stderr,"pipe write error\n");
			}
			close(piped[1]);
		}
		if (gMountOptions.meta == 0) {
			LizardClient::fs_term();
		} else {
			masterproxy_term();
			fs_term();
			symlink_cache_term();
		}
		return 1;
	}

	fuse_session_add_chan(se, ch);

	if (fuse_set_signal_handlers(se)<0) {
		fprintf(stderr,"error in fuse_set_signal_handlers\n");
		fuse_session_remove_chan(ch);
		fuse_session_destroy(se);
		fuse_unmount(mp,ch);
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				fprintf(stderr,"pipe write error\n");
			}
			close(piped[1]);
		}
		if (gMountOptions.meta == 0) {
			LizardClient::fs_term();
		} else {
			masterproxy_term();
			fs_term();
			symlink_cache_term();
		}
		return 1;
	}

	if (gMountOptions.debug==0 && fg==0) {
		setsid();
		setpgid(0,getpid());
		if ((i = open("/dev/null", O_RDWR, 0)) != -1) {
			(void)dup2(i, STDIN_FILENO);
			(void)dup2(i, STDOUT_FILENO);
			(void)dup2(i, STDERR_FILENO);
			if (i>2) close (i);
		}
	}

	if (mt) {
		err = fuse_session_loop_mt(se);
	} else {
		err = fuse_session_loop(se);
	}
	if (err) {
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				lzfs_pretty_syslog(LOG_ERR,"pipe write error: %s",strerr(errno));
			}
			close(piped[1]);
		}
	}
	fuse_remove_signal_handlers(se);
	fuse_session_remove_chan(ch);
	fuse_session_destroy(se);
	fuse_unmount(mp,ch);
	if (gMountOptions.meta == 0) {
		LizardClient::fs_term();
	} else {
		masterproxy_term();
		fs_term();
		symlink_cache_term();
	}
	return err ? 1 : 0;
}

#if FUSE_VERSION == 25
static int fuse_opt_insert_arg(struct fuse_args *args, int pos, const char *arg) {
	assert(pos <= args->argc);
	if (fuse_opt_add_arg(args, arg) == -1) {
		return -1;
	}
	if (pos != args->argc - 1) {
		char *newarg = args->argv[args->argc - 1];
		memmove(&args->argv[pos + 1], &args->argv[pos], sizeof(char *) * (args->argc - pos - 1));
		args->argv[pos] = newarg;
	}
	return 0;
}
#endif

static unsigned int strncpy_remove_commas(char *dstbuff, unsigned int dstsize,char *src) {
	char c;
	unsigned int l;
	l=0;
	while ((c=*src++) && l+1<dstsize) {
		if (c!=',') {
			*dstbuff++ = c;
			l++;
		}
	}
	*dstbuff=0;
	return l;
}

#if LIZARDFS_HAVE_FUSE_VERSION
static unsigned int strncpy_escape_commas(char *dstbuff, unsigned int dstsize,char *src) {
	char c;
	unsigned int l;
	l=0;
	while ((c=*src++) && l+1<dstsize) {
		if (c!=',' && c!='\\') {
			*dstbuff++ = c;
			l++;
		} else {
			if (l+2<dstsize) {
				*dstbuff++ = '\\';
				*dstbuff++ = c;
				l+=2;
			} else {
				*dstbuff=0;
				return l;
			}
		}
	}
	*dstbuff=0;
	return l;
}
#endif

void make_fsname(struct fuse_args *args) {
	char fsnamearg[256];
	unsigned int l;
#if LIZARDFS_HAVE_FUSE_VERSION
	int libver;
	libver = fuse_version();
	if (libver >= 27) {
		l = snprintf(fsnamearg,256,"-osubtype=mfs%s,fsname=",(gMountOptions.meta)?"meta":"");
		if (libver >= 28) {
			l += strncpy_escape_commas(fsnamearg+l,256-l,gMountOptions.masterhost);
			if (l<255) {
				fsnamearg[l++]=':';
			}
			l += strncpy_escape_commas(fsnamearg+l,256-l,gMountOptions.masterport);
			if (gMountOptions.subfolder[0]!='/') {
				if (l<255) {
					fsnamearg[l++]='/';
				}
			}
			if (gMountOptions.subfolder[0]!='/' && gMountOptions.subfolder[1]!=0) {
				l += strncpy_escape_commas(fsnamearg+l,256-l,gMountOptions.subfolder);
			}
			if (l>255) {
				l=255;
			}
			fsnamearg[l]=0;
		} else {
			l += strncpy_remove_commas(fsnamearg+l,256-l,gMountOptions.masterhost);
			if (l<255) {
				fsnamearg[l++]=':';
			}
			l += strncpy_remove_commas(fsnamearg+l,256-l,gMountOptions.masterport);
			if (gMountOptions.subfolder[0]!='/') {
				if (l<255) {
					fsnamearg[l++]='/';
				}
			}
			if (gMountOptions.subfolder[0]!='/' && gMountOptions.subfolder[1]!=0) {
				l += strncpy_remove_commas(fsnamearg+l,256-l,gMountOptions.subfolder);
			}
			if (l>255) {
				l=255;
			}
			fsnamearg[l]=0;
		}
	} else {
#else
		l = snprintf(fsnamearg,256,"-ofsname=mfs%s#",(gMountOptions.meta)?"meta":"");
		l += strncpy_remove_commas(fsnamearg+l,256-l,gMountOptions.masterhost);
		if (l<255) {
			fsnamearg[l++]=':';
		}
		l += strncpy_remove_commas(fsnamearg+l,256-l,gMountOptions.masterport);
		if (gMountOptions.subfolder[0]!='/') {
			if (l<255) {
				fsnamearg[l++]='/';
			}
		}
		if (gMountOptions.subfolder[0]!='/' && gMountOptions.subfolder[1]!=0) {
			l += strncpy_remove_commas(fsnamearg+l,256-l,gMountOptions.subfolder);
		}
		if (l>255) {
			l=255;
		}
		fsnamearg[l]=0;
#endif
#if LIZARDFS_HAVE_FUSE_VERSION
	}
#endif
	fuse_opt_insert_arg(args, 1, fsnamearg);
}

int main(int argc, char *argv[]) try {
	int res;
	int mt,fg;
	char *mountpoint;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_args defaultargs = FUSE_ARGS_INIT(0, NULL);

	fuse_opt_add_arg(&defaultargs,"fakeappname");

	if (read_masterhost_if_present(&args))
		exit(1);

	if (fuse_opt_parse(&args, &defaultargs, gMfsOptsStage1, mfs_opt_proc_stage1))
		exit(1);

	if (!gCustomCfg)
		mfs_opt_parse_cfg_file(DEFAULT_MFSMOUNT_CONFIG_PATH, 1, &defaultargs);

	if (fuse_opt_parse(&defaultargs, &gMountOptions, gMfsOptsStage2, mfs_opt_proc_stage2))
		exit(1);

	if (fuse_opt_parse(&args, &gMountOptions, gMfsOptsStage2, mfs_opt_proc_stage2))
		exit(1);

#if FUSE_VERSION >= 30
	struct fuse_conn_info_opts *conn_opts;
	conn_opts = fuse_parse_conn_info_opts(&args);
	if (!conn_opts) {
		exit(1);
	}

	init_fuse_lowlevel_ops();

	if (gMountOptions.cachemode!=NULL && gMountOptions.cachefiles) {
		fprintf(stderr,"mfscachemode and mfscachefiles options are exclusive - use only mfscachemode\nsee: %s -h for help\n",argv[0]);
		return 1;
	}

	if (gMountOptions.cachemode==NULL) {
		gMountOptions.keepcache=(gMountOptions.cachefiles)?1:0;
	} else if (strcasecmp(gMountOptions.cachemode,"AUTO")==0) {
		gMountOptions.keepcache=0;
	} else if (strcasecmp(gMountOptions.cachemode,"YES")==0 || strcasecmp(gMountOptions.cachemode,"ALWAYS")==0) {
		gMountOptions.keepcache=1;
	} else if (strcasecmp(gMountOptions.cachemode,"NO")==0
			|| strcasecmp(gMountOptions.cachemode,"NONE")==0
			|| strcasecmp(gMountOptions.cachemode,"NEVER")==0) {
		gMountOptions.keepcache=2;
		gMountOptions.cacheexpirationtime=0;
	} else {
		fprintf(stderr,"unrecognized cachemode option\nsee: %s -h for help\n",argv[0]);
		return 1;
	}
	if (gMountOptions.sugidclearmodestr==NULL) {
		gMountOptions.sugidclearmode = LizardClient::FsInitParams::kDefaultSugidClearMode;
	} else if (strcasecmp(gMountOptions.sugidclearmodestr,"NEVER")==0) {
		gMountOptions.sugidclearmode = SugidClearMode::kNever;
	} else if (strcasecmp(gMountOptions.sugidclearmodestr,"ALWAYS")==0) {
		gMountOptions.sugidclearmode = SugidClearMode::kAlways;
	} else if (strcasecmp(gMountOptions.sugidclearmodestr,"OSX")==0) {
		gMountOptions.sugidclearmode = SugidClearMode::kOsx;
	} else if (strcasecmp(gMountOptions.sugidclearmodestr,"BSD")==0) {
		gMountOptions.sugidclearmode = SugidClearMode::kBsd;
	} else if (strcasecmp(gMountOptions.sugidclearmodestr,"EXT")==0) {
		gMountOptions.sugidclearmode = SugidClearMode::kExt;
	} else if (strcasecmp(gMountOptions.sugidclearmodestr,"XFS")==0) {
		gMountOptions.sugidclearmode = SugidClearMode::kXfs;
	} else {
		fprintf(stderr,"unrecognized sugidclearmode option\nsee: %s -h for help\n",argv[0]);
		return 1;
	}
	if (gMountOptions.masterhost==NULL) {
		gMountOptions.masterhost = strdup("mfsmaster");
	}
	if (gMountOptions.masterport==NULL) {
		gMountOptions.masterport = strdup("9421");
	}
	if (gMountOptions.subfolder==NULL) {
		gMountOptions.subfolder = strdup("/");
	}
	if (gMountOptions.nofile==0) {
		gMountOptions.nofile=100000;
	}
	if (gMountOptions.writecachesize==0) {
		gMountOptions.writecachesize=128;
	}
	if (gMountOptions.cachePerInodePercentage < 1) {
		fprintf(stderr, "cache per inode percentage too low (%u %%) - increased to 1%%\n",
				gMountOptions.cachePerInodePercentage);
		gMountOptions.cachePerInodePercentage = 1;
	}
	if (gMountOptions.cachePerInodePercentage > 100) {
		fprintf(stderr, "cache per inode percentage too big (%u %%) - decreased to 100%%\n",
				gMountOptions.cachePerInodePercentage);
		gMountOptions.cachePerInodePercentage = 100;
	}
	if (gMountOptions.writecachesize<16) {
		fprintf(stderr,"write cache size too low (%u MiB) - increased to 16 MiB\n",
				gMountOptions.writecachesize);
		gMountOptions.writecachesize=16;
	}
	if (gMountOptions.writecachesize>1024*1024) {
		fprintf(stderr,"write cache size too big (%u MiB) - decreased to 1 TiB\n",
				gMountOptions.writecachesize);
		gMountOptions.writecachesize=1024*1024;
	}
	if (gMountOptions.writeworkers<1) {
		fprintf(stderr,"no write workers - increasing number of workers to 1\n");
		gMountOptions.writeworkers=1;
	}
	if (gMountOptions.writewindowsize < 1) {
		fprintf(stderr,"write window size is 0 - increasing to 1\n");
		gMountOptions.writewindowsize = 1;
	}
	if (gMountOptions.nostdmountoptions==0) {
		fuse_opt_add_arg(&args, "-o" DEFAULT_OPTIONS);
	}
	if (gMountOptions.aclcachesize > 1000 * 1000) {
		fprintf(stderr,"acl cache size too big (%u) - decreased to 1000000\n",
				gMountOptions.aclcachesize);
		gMountOptions.aclcachesize = 1000 * 1000;
	}
	if (gMountOptions.direntrycachesize > 10000000) {
		fprintf(stderr,"directory entry cache size too big (%u) - decreased to 10000000\n",
				gMountOptions.direntrycachesize);
		gMountOptions.direntrycachesize = 10000000;
	}

	make_fsname(&args);

	if (fuse_parse_cmdline(&args,&mountpoint,&mt,&fg)<0) {
		fprintf(stderr,"see: %s -h for help\n",argv[0]);
		return 1;
	}

	if (!mountpoint) {
		if (gDefaultMountpoint) {
			mountpoint = gDefaultMountpoint;
		} else {
			fprintf(stderr,"no mount point\nsee: %s -h for help\n",argv[0]);
			return 1;
		}
	}

	res = mainloop(&args,mountpoint,mt,fg);
	fuse_opt_free_args(&args);
	fuse_opt_free_args(&defaultargs);
	free(gMountOptions.masterhost);
	free(gMountOptions.masterport);
	if (gMountOptions.bindhost) {
		free(gMountOptions.bindhost);
	}
	free(gMountOptions.subfolder);
	if (gDefaultMountpoint && gDefaultMountpoint != mountpoint) {
		free(gDefaultMountpoint);
	}
	if (gMountOptions.iolimits) {
		free(gMountOptions.iolimits);
	}
	free(mountpoint);
	stats_term();
	return res;
} catch (std::bad_alloc ex) {
	mabort("run out of memory");
}
