/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

   This file was part of LizardFS and is part of LizardFS.

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
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <syslog.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
#include <fstream>

#include <fuse.h>
#include <fuse/fuse_lowlevel.h>
#include <fuse/fuse_opt.h>

#include "common/crc.h"
#include "common/md5.h"
#include "common/LFSCommunication.h"
#include "common/lfserr.h"
#include "mount/csdb.h"
#include "mount/fuse/lfs_fuse.h"
#include "mount/fuse/lfs_meta_fuse.h"
#include "mount/g_io_limiters.h"
#include "mount/mastercomm.h"
#include "mount/masterproxy.h"
#include "mount/readdata.h"
#include "mount/stats.h"
#include "mount/symlinkcache.h"
#include "mount/writedata.h"

#if defined(LIZARDFS_HAVE_MLOCKALL) && defined(RLIMIT_MEMLOCK) && defined(MCL_CURRENT) && defined(MCL_FUTURE)
#  define LFS_USE_MEMLOCK
#endif

#ifdef LFS_USE_MEMLOCK
#  include <sys/mman.h>
#endif

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)

#if defined(__APPLE__)
#define DEFAULT_OPTIONS "allow_other,default_permissions,daemon_timeout=600,iosize=65536"
#else
#define DEFAULT_OPTIONS "allow_other,default_permissions"
#endif

static void lfs_fsinit (void *userdata, struct fuse_conn_info *conn);

static struct fuse_lowlevel_ops lfs_meta_oper;

static struct fuse_lowlevel_ops lfs_oper;

static void init_fuse_lowlevel_ops() {
   lfs_meta_oper.init = lfs_fsinit;
   lfs_meta_oper.statfs =   lfs_meta_statfs;
   lfs_meta_oper.lookup =      lfs_meta_lookup;
   lfs_meta_oper.getattr =  lfs_meta_getattr;
   lfs_meta_oper.setattr =  lfs_meta_setattr;
   lfs_meta_oper.unlink =      lfs_meta_unlink;
   lfs_meta_oper.rename =      lfs_meta_rename;
   lfs_meta_oper.opendir =  lfs_meta_opendir;
   lfs_meta_oper.readdir =  lfs_meta_readdir;
   lfs_meta_oper.releasedir =  lfs_meta_releasedir;
   lfs_meta_oper.open =     lfs_meta_open;
   lfs_meta_oper.release =  lfs_meta_release;
   lfs_meta_oper.read =     lfs_meta_read;
   lfs_meta_oper.write =       lfs_meta_write;

   lfs_oper.init           = lfs_fsinit;
   lfs_oper.statfs     = lfs_statfs;
   lfs_oper.lookup     = lfs_lookup;
   lfs_oper.getattr = lfs_getattr;
   lfs_oper.setattr = lfs_setattr;
   lfs_oper.mknod      = lfs_mknod;
   lfs_oper.unlink     = lfs_unlink;
   lfs_oper.mkdir      = lfs_mkdir;
   lfs_oper.rmdir      = lfs_rmdir;
   lfs_oper.symlink = lfs_symlink;
   lfs_oper.readlink   = lfs_readlink;
   lfs_oper.rename     = lfs_rename;
   lfs_oper.link    = lfs_link;
   lfs_oper.opendir = lfs_opendir;
   lfs_oper.readdir = lfs_readdir;
   lfs_oper.releasedir = lfs_releasedir;
   lfs_oper.create     = lfs_create;
   lfs_oper.open    = lfs_open;
   lfs_oper.release = lfs_release;
   lfs_oper.flush      = lfs_flush;
   lfs_oper.fsync      = lfs_fsync;
   lfs_oper.read    = lfs_read;
   lfs_oper.write      = lfs_write;
   lfs_oper.access     = lfs_access;
   lfs_oper.getxattr       = lfs_getxattr;
   lfs_oper.setxattr       = lfs_setxattr;
   lfs_oper.listxattr      = lfs_listxattr;
   lfs_oper.removexattr    = lfs_removexattr;
}

struct lfsopts {
	char *masterhost;
	char *masterport;
	char *bindhost;
	char *subfolder;
	char *password;
	char *md5pass;
	unsigned nofile;
	signed nice;
#ifdef LFS_USE_MEMLOCK
	int memlock;
#endif
	int nostdmountoptions;
	int meta;
	int debug;
	int delayedinit;
	int acl;
	int mkdircopysgid;
	char *sugidclearmodestr;
	SugidClearMode sugidclearmode;
	char *cachemode;
	int cachefiles;
	int keepcache;
	int passwordask;
	int donotrememberpassword;
	unsigned writecachesize;
	unsigned ioretries;
	unsigned aclcachesize;
	double attrcacheto;
	double entrycacheto;
	double direntrycacheto;
	double aclcacheto;
	unsigned reportreservedperiod;
	char *iolimits;
};

static struct lfsopts lfsopts;
static char *defaultmountpoint = NULL;

static int custom_cfg;

enum {
	KEY_CFGFILE,
	KEY_META,
	KEY_HOST,
	KEY_PORT,
	KEY_BIND,
	KEY_PATH,
	KEY_PASSWORDASK,
	KEY_NOSTDMOUNTOPTIONS,
	KEY_HELP,
	KEY_VERSION
};

#define LFS_OPT(t, p, v) { t, offsetof(struct lfsopts, p), v }

static struct fuse_opt lfs_opts_stage1[] = {
	FUSE_OPT_KEY("lfscfgfile=",    KEY_CFGFILE),
	FUSE_OPT_KEY("-c ",            KEY_CFGFILE),
	FUSE_OPT_END
};

static struct fuse_opt lfs_opts_stage2[] = {
	LFS_OPT("lfsmaster=%s", masterhost, 0),
	LFS_OPT("lfsport=%s", masterport, 0),
	LFS_OPT("lfsbind=%s", bindhost, 0),
	LFS_OPT("lfssubfolder=%s", subfolder, 0),
	LFS_OPT("lfspassword=%s", password, 0),
	LFS_OPT("lfsmd5pass=%s", md5pass, 0),
	LFS_OPT("lfsrlimitnofile=%u", nofile, 0),
	LFS_OPT("lfsnice=%d", nice, 0),
#ifdef LFS_USE_MEMLOCK
	LFS_OPT("lfsmemlock", memlock, 1),
#endif
	LFS_OPT("lfswritecachesize=%u", writecachesize, 0),
	LFS_OPT("lfsaclcachesize=%u", aclcachesize, 0),
	LFS_OPT("lfsioretries=%u", ioretries, 0),
	LFS_OPT("lfsdebug", debug, 1),
	LFS_OPT("lfsmeta", meta, 1),
	LFS_OPT("lfsdelayedinit", delayedinit, 1),
	LFS_OPT("lfsacl", acl, 1),
	LFS_OPT("lfsdonotrememberpassword", donotrememberpassword, 1),
	LFS_OPT("lfscachefiles", cachefiles, 1),
	LFS_OPT("lfscachemode=%s", cachemode, 0),
	LFS_OPT("lfsmkdircopysgid=%u", mkdircopysgid, 0),
	LFS_OPT("lfssugidclearmode=%s", sugidclearmodestr, 0),
	LFS_OPT("lfsattrcacheto=%lf", attrcacheto, 0),
	LFS_OPT("lfsentrycacheto=%lf", entrycacheto, 0),
	LFS_OPT("lfsdirentrycacheto=%lf", direntrycacheto, 0),
	LFS_OPT("lfsaclcacheto=%lf", aclcacheto, 0),
	LFS_OPT("lfsreportreservedperiod=%u", reportreservedperiod, 0),
	LFS_OPT("lfsiolimits=%s", iolimits, 0),

	FUSE_OPT_KEY("-m",             KEY_META),
	FUSE_OPT_KEY("--meta",         KEY_META),
	FUSE_OPT_KEY("-H ",            KEY_HOST),
	FUSE_OPT_KEY("-P ",            KEY_PORT),
	FUSE_OPT_KEY("-B ",            KEY_BIND),
	FUSE_OPT_KEY("-S ",            KEY_PATH),
	FUSE_OPT_KEY("-p",             KEY_PASSWORDASK),
	FUSE_OPT_KEY("--password",     KEY_PASSWORDASK),
	FUSE_OPT_KEY("-n",             KEY_NOSTDMOUNTOPTIONS),
	FUSE_OPT_KEY("--nostdopts",    KEY_NOSTDMOUNTOPTIONS),
	FUSE_OPT_KEY("-V",             KEY_VERSION),
	FUSE_OPT_KEY("--version",      KEY_VERSION),
	FUSE_OPT_KEY("-h",             KEY_HELP),
	FUSE_OPT_KEY("--help",         KEY_HELP),
	FUSE_OPT_END
};

static void usage(const char *progname) {
	fprintf(stderr,
"usage: %s mountpoint [options]\n"
"\n", progname);
	fprintf(stderr,
"general options:\n"
"    -o opt,[opt...]         mount options\n"
"    -h   --help             print help\n"
"    -V   --version          print version\n"
"\n");
	fprintf(stderr,
"LFS options:\n"
"    -c CFGFILE                  equivalent to '-o lfscfgfile=CFGFILE'\n"
"    -m   --meta                 equivalent to '-o lfsmeta'\n"
"    -H HOST                     equivalent to '-o lfsmaster=HOST'\n"
"    -P PORT                     equivalent to '-o lfsport=PORT'\n"
"    -B IP                       equivalent to '-o lfsbind=IP'\n"
"    -S PATH                     equivalent to '-o lfssubfolder=PATH'\n"
"    -p   --password             similar to '-o lfspassword=PASSWORD', but show prompt and ask user for password\n"
"    -n   --nostdopts            do not add standard LFS mount options: '-o " DEFAULT_OPTIONS ",fsname=LFS'\n"
"    -o lfscfgfile=CFGFILE       load some mount options from external file (if not specified then use default file: " ETC_PATH "/lfs/lfsmount.cfg or " ETC_PATH "/lfsmount.cfg)\n"
"    -o lfsdebug                 print some debugging information\n"
"    -o lfsmeta                  mount meta filesystem (trash etc.)\n"
"    -o lfsdelayedinit           connection with master is done in background - with this option mount can be run without network (good for being run from fstab / init scripts etc.)\n"
"    -o lfsacl                   enable ACL support (disabled by default)\n"
#ifdef __linux__
"    -o lfsmkdircopysgid=N       sgid bit should be copied during mkdir operation (default: 1)\n"
#else
"    -o lfsmkdircopysgid=N       sgid bit should be copied during mkdir operation (default: 0)\n"
#endif
#if defined(DEFAULT_SUGID_CLEAR_MODE_EXT)
"    -o lfssugidclearmode=SMODE  set sugid clear mode (see below ; default: EXT)\n"
#elif defined(DEFAULT_SUGID_CLEAR_MODE_BSD)
"    -o lfssugidclearmode=SMODE  set sugid clear mode (see below ; default: BSD)\n"
#elif defined(DEFAULT_SUGID_CLEAR_MODE_OSX)
"    -o lfssugidclearmode=SMODE  set sugid clear mode (see below ; default: OSX)\n"
#else
"    -o lfssugidclearmode=SMODE  set sugid clear mode (see below ; default: NEVER)\n"
#endif
"    -o lfscachemode=CMODE       set cache mode (see below ; default: AUTO)\n"
"    -o lfscachefiles            (deprecated) equivalent to '-o lfscachemode=YES'\n"
"    -o lfsattrcacheto=SEC       set attributes cache timeout in seconds (default: 1.0)\n"
"    -o lfsentrycacheto=SEC      set file entry cache timeout in seconds (default: 0.0)\n"
"    -o lfsdirentrycacheto=SEC   set directory entry cache timeout in seconds (default: 1.0)\n"
"    -o lfsaclcacheto=SEC        set ACL cache timeout in seconds (default: 1.0)\n"
"    -o lfsreportreservedperiod=SEC set reporting reserved inodes interval in seconds (default: 60)\n"
"    -o lfsrlimitnofile=N        on startup lfsmount tries to change number of descriptors it can simultaneously open (default: 100000)\n"
"    -o lfsnice=N                on startup lfsmount tries to change his 'nice' value (default: -19)\n"
#ifdef LFS_USE_MEMLOCK
"    -o lfsmemlock               try to lock memory\n"
#endif
"    -o lfswritecachesize=N      define size of write cache in MiB (default: 128)\n"
"    -o lfsaclcachesize=N        define ACL cache size in number of entries (0: no cache; default: 1000)\n"
"    -o lfsioretries=N           define number of retries before I/O error is returned (default: 30)\n"
"    -o lfsmaster=HOST           define lfsmaster location (default: lfsmaster)\n"
"    -o lfsport=PORT             define lfsmaster port number (default: 9421)\n"
"    -o lfsbind=IP               define source ip address for connections (default: NOT DEFINED - choosen automatically by OS)\n"
"    -o lfssubfolder=PATH        define subfolder to mount as root (default: /)\n"
"    -o lfspassword=PASSWORD     authenticate to lfsmaster with password\n"
"    -o lfsmd5pass=MD5           authenticate to lfsmaster using directly given md5 (only if lfspassword is not defined)\n"
"    -o lfsdonotrememberpassword do not remember password in memory - more secure, but when session is lost then new session is created without password\n"
"    -o lfsiolimits=FILE         define I/O limits configuration file\n"
"\n");
	fprintf(stderr,
"CMODE can be set to:\n"
"    NO,NONE or NEVER            never allow files data to be kept in cache (safest but can reduce efficiency)\n"
"    YES or ALWAYS               always allow files data to be kept in cache (dangerous)\n"
"    AUTO                        file cache is managed by lfsmaster automatically (should be very safe and efficient)\n"
"\n");
	fprintf(stderr,
"SMODE can be set to:\n"
"    NEVER                       LFS will not change suid and sgid bit on chown\n"
"    ALWAYS                      clear suid and sgid on every chown - safest operation\n"
"    OSX                         standard behavior in OS X and Solaris (chown made by unprivileged user clear suid and sgid)\n"
"    BSD                         standard behavior in *BSD systems (like in OSX, but only when something is really changed)\n"
"    EXT                         standard behavior in most file systems on Linux (directories not changed, others: suid cleared always, sgid only when group exec bit is set)\n"
"    XFS                         standard behavior in XFS on Linux (like EXT but directories are changed by unprivileged users)\n"
"SMODE extra info:\n"
"    btrfs,ext2,ext3,ext4,hfs[+],jfs,ntfs and reiserfs on Linux work as 'EXT'.\n"
"    Only xfs on Linux works a little different. Beware that there is a strange\n"
"    operation - chown(-1,-1) which is usually converted by a kernel into something\n"
"    like 'chmod ug-s', and therefore can't be controlled by LFS as 'chown'\n"
"\n");
}

static void lfs_opt_parse_cfg_file(const char *filename,int optional,struct fuse_args *outargs) {
	FILE *fd;
	char lbuff[1000],*p;

	fd = fopen(filename,"r");
	if (fd==NULL) {
		if (optional==0) {
			fprintf(stderr,"can't open cfg file: %s\n",filename);
			abort();
		}
		return;
	}
	custom_cfg = 1;
	while (fgets(lbuff,999,fd)) {
		if (lbuff[0]!='#' && lbuff[0]!=';') {
			lbuff[999]=0;
			for (p = lbuff ; *p ; p++) {
				if (*p=='\r' || *p=='\n') {
					*p=0;
					break;
				}
			}
			p--;
			while (p>=lbuff && (*p==' ' || *p=='\t')) {
				*p=0;
				p--;
			}
			p = lbuff;
			while (*p==' ' || *p=='\t') {
				p++;
			}
			if (*p) {
				if (*p=='-') {
					fuse_opt_add_arg(outargs,p);
				} else if (*p=='/') {
					if (defaultmountpoint) {
						free(defaultmountpoint);
					}
					defaultmountpoint = strdup(p);
				} else {
					fuse_opt_add_arg(outargs,"-o");
					fuse_opt_add_arg(outargs,p);
				}
			}
		}
	}
	fclose(fd);
}

static int lfs_opt_proc_stage1(void *data, const char *arg, int key, struct fuse_args *outargs) {
	struct fuse_args *defargs = (struct fuse_args*)data;
	(void)outargs;

	if (key==KEY_CFGFILE) {
		if (memcmp(arg,"lfscfgfile=",11)==0) {
			lfs_opt_parse_cfg_file(arg+11,0,defargs);
		} else if (arg[0]=='-' && arg[1]=='c') {
			lfs_opt_parse_cfg_file(arg+2,0,defargs);
		}
		return 0;
	}
	return 1;
}

// return value:
//   0 - discard this arg
//   1 - keep this arg for future processing
static int lfs_opt_proc_stage2(void *data, const char *arg, int key, struct fuse_args *outargs) {
	(void)data;

	switch (key) {
	case FUSE_OPT_KEY_OPT:
		return 1;
	case FUSE_OPT_KEY_NONOPT:
		return 1;
	case KEY_HOST:
		if (lfsopts.masterhost!=NULL) {
			free(lfsopts.masterhost);
		}
		lfsopts.masterhost = strdup(arg+2);
		return 0;
	case KEY_PORT:
		if (lfsopts.masterport!=NULL) {
			free(lfsopts.masterport);
		}
		lfsopts.masterport = strdup(arg+2);
		return 0;
	case KEY_BIND:
		if (lfsopts.bindhost!=NULL) {
			free(lfsopts.bindhost);
		}
		lfsopts.bindhost = strdup(arg+2);
		return 0;
	case KEY_PATH:
		if (lfsopts.subfolder!=NULL) {
			free(lfsopts.subfolder);
		}
		lfsopts.subfolder = strdup(arg+2);
		return 0;
	case KEY_PASSWORDASK:
		lfsopts.passwordask = 1;
		return 0;
	case KEY_META:
		lfsopts.meta = 1;
		return 0;
	case KEY_NOSTDMOUNTOPTIONS:
		lfsopts.nostdmountoptions = 1;
		return 0;
	case KEY_VERSION:
		fprintf(stderr, "LFS version %s\n", LIZARDFS_PACKAGE_VERSION);
		{
			struct fuse_args helpargs = FUSE_ARGS_INIT(0, NULL);

			fuse_opt_add_arg(&helpargs,outargs->argv[0]);
			fuse_opt_add_arg(&helpargs,"--version");
			fuse_parse_cmdline(&helpargs,NULL,NULL,NULL);
			fuse_unmount(NULL, fuse_mount(NULL, &helpargs));
		}
		exit(0);
	case KEY_HELP:
		usage(outargs->argv[0]);
		{
			struct fuse_args helpargs = FUSE_ARGS_INIT(0, NULL);

			fuse_opt_add_arg(&helpargs,outargs->argv[0]);
			fuse_opt_add_arg(&helpargs,"-ho");
			fuse_parse_cmdline(&helpargs,NULL,NULL,NULL);
			fuse_unmount(NULL, fuse_mount(NULL, &helpargs));
		}
		exit(1);
	default:
		fprintf(stderr, "internal error\n");
		abort();
	}
}

static void lfs_fsinit (void *userdata, struct fuse_conn_info *conn) {
	conn->want |= FUSE_CAP_DONT_MASK;

	int *piped = (int*)userdata;
	if (piped[1]>=0) {
		char s = 0;
		if (write(piped[1],&s,1)!=1) {
			syslog(LOG_ERR,"pipe write error: %s",strerr(errno));
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
	uint8_t md5pass[16];

	if (lfsopts.passwordask && lfsopts.password==NULL && lfsopts.md5pass==NULL) {
		lfsopts.password = getpass("LFS Password:");
	}
	if (lfsopts.password) {
		md5_init(&ctx);
		md5_update(&ctx,(uint8_t*)(lfsopts.password),strlen(lfsopts.password));
		md5_final(md5pass,&ctx);
		memset(lfsopts.password,0,strlen(lfsopts.password));
	} else if (lfsopts.md5pass) {
		uint8_t *p = (uint8_t*)(lfsopts.md5pass);
		for (i=0 ; i<16 ; i++) {
			if (*p>='0' && *p<='9') {
				md5pass[i]=(*p-'0')<<4;
			} else if (*p>='a' && *p<='f') {
				md5pass[i]=(*p-'a'+10)<<4;
			} else if (*p>='A' && *p<='F') {
				md5pass[i]=(*p-'A'+10)<<4;
			} else {
				fprintf(stderr,"bad md5 definition (md5 should be given as 32 hex digits)\n");
				return 1;
			}
			p++;
			if (*p>='0' && *p<='9') {
				md5pass[i]+=(*p-'0');
			} else if (*p>='a' && *p<='f') {
				md5pass[i]+=(*p-'a'+10);
			} else if (*p>='A' && *p<='F') {
				md5pass[i]+=(*p-'A'+10);
			} else {
				fprintf(stderr,"bad md5 definition (md5 should be given as 32 hex digits)\n");
				return 1;
			}
			p++;
		}
		if (*p) {
			fprintf(stderr,"bad md5 definition (md5 should be given as 32 hex digits)\n");
			return 1;
		}
		memset(lfsopts.md5pass,0,strlen(lfsopts.md5pass));
	}

	if (lfsopts.delayedinit) {
		fs_init_master_connection(lfsopts.bindhost, lfsopts.masterhost, lfsopts.masterport,
				lfsopts.meta, mp, lfsopts.subfolder,
				(lfsopts.password || lfsopts.md5pass) ? md5pass : NULL,
				lfsopts.donotrememberpassword, 1, lfsopts.ioretries, lfsopts.reportreservedperiod);
	} else {
		if (fs_init_master_connection(lfsopts.bindhost, lfsopts.masterhost, lfsopts.masterport,
					lfsopts.meta, mp, lfsopts.subfolder,
					(lfsopts.password || lfsopts.md5pass) ? md5pass : NULL,
					lfsopts.donotrememberpassword, 0, lfsopts.ioretries,
					lfsopts.reportreservedperiod) < 0) {
			return 1;
		}
	}
	memset(md5pass,0,16);

	if (fg==0) {
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY , LOG_DAEMON);
	} else {
#if defined(LOG_PERROR)
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY | LOG_PERROR, LOG_USER);
#else
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY, LOG_USER);
#endif
	}

	rls.rlim_cur = lfsopts.nofile;
	rls.rlim_max = lfsopts.nofile;
	setrlimit(RLIMIT_NOFILE,&rls);

	setpriority(PRIO_PROCESS,getpid(),lfsopts.nice);
#ifdef LFS_USE_MEMLOCK
	if (lfsopts.memlock) {
		rls.rlim_cur = RLIM_INFINITY;
		rls.rlim_max = RLIM_INFINITY;
		if (setrlimit(RLIMIT_MEMLOCK,&rls)<0) {
			lfsopts.memlock=0;
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
		} else if (err>0) {
			close(piped[1]);
			err = read(piped[0],&s,1);
			if (err==0) {
				s=1;
			}
			return s;
		}
		close(piped[0]);
		s=1;
	}


#ifdef LFS_USE_MEMLOCK
	if (lfsopts.memlock) {
		if (mlockall(MCL_CURRENT|MCL_FUTURE)==0) {
			syslog(LOG_NOTICE,"process memory was successfully locked in RAM");
		}
	}
#endif

	symlink_cache_init();
	if (lfsopts.meta == 0) {
		// initialize the global IO limiter before starting mastercomm threads
		gGlobalIoLimiter();
	}
	fs_init_threads(lfsopts.ioretries);
	masterproxy_init();

	if (lfsopts.meta==0) {
		try {
			IoLimitsConfigLoader loader;
			if (lfsopts.iolimits) {
				loader.load(std::ifstream(lfsopts.iolimits));
			}
			// initialize the local limiter before loading configuration
			gLocalIoLimiter();
			gMountLimiter().loadConfiguration(loader);
		} catch (Exception& ex) {
			fprintf(stderr, "Can't initialize I/O limiting: %s", ex.what());
			masterproxy_term();
			fs_term();
			symlink_cache_term();
			return 1;
		}
		csdb_init();
		read_data_init(lfsopts.ioretries);
		write_data_init(lfsopts.writecachesize*1024*1024,lfsopts.ioretries);
	}

	ch = fuse_mount(mp, args);
	if (ch==NULL) {
		fprintf(stderr,"error in fuse_mount\n");
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				fprintf(stderr,"pipe write error\n");
			}
			close(piped[1]);
		}
		if (lfsopts.meta==0) {
			write_data_term();
			read_data_term();
			csdb_term();
		}
		masterproxy_term();
		fs_term();
		symlink_cache_term();
		return 1;
	}

	if (lfsopts.meta) {
		lfs_meta_init(lfsopts.debug,lfsopts.entrycacheto,lfsopts.attrcacheto);
		se = fuse_lowlevel_new(args, &lfs_meta_oper, sizeof(lfs_meta_oper), (void*)piped);
	} else {
		lfs_init(lfsopts.debug, lfsopts.keepcache, lfsopts.direntrycacheto, lfsopts.entrycacheto,
				lfsopts.attrcacheto, lfsopts.mkdircopysgid, lfsopts.sugidclearmode, lfsopts.acl,
				lfsopts.aclcacheto, lfsopts.aclcachesize);
		se = fuse_lowlevel_new(args, &lfs_oper, sizeof(lfs_oper), (void*)piped);
	}
	if (se==NULL) {
		fuse_unmount(mp,ch);
		fprintf(stderr,"error in fuse_lowlevel_new\n");
		usleep(100000); // time for print other error messages by FUSE
		if (piped[1]>=0) {
			if (write(piped[1],&s,1)!=1) {
				fprintf(stderr,"pipe write error\n");
			}
			close(piped[1]);
		}
		if (lfsopts.meta==0) {
			write_data_term();
			read_data_term();
			csdb_term();
		}
		masterproxy_term();
		fs_term();
		symlink_cache_term();
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
		if (lfsopts.meta==0) {
			write_data_term();
			read_data_term();
			csdb_term();
		}
		masterproxy_term();
		fs_term();
		symlink_cache_term();
		return 1;
	}

	if (lfsopts.debug==0 && fg==0) {
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
				syslog(LOG_ERR,"pipe write error: %s",strerr(errno));
			}
			close(piped[1]);
		}
	}
	fuse_remove_signal_handlers(se);
	fuse_session_remove_chan(ch);
	fuse_session_destroy(se);
	fuse_unmount(mp,ch);
	if (lfsopts.meta==0) {
		write_data_term();
		read_data_term();
		csdb_term();
	}
	masterproxy_term();
	fs_term();
	symlink_cache_term();
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
		l = snprintf(fsnamearg,256,"-osubtype=lfs%s,fsname=",(lfsopts.meta)?"meta":"");
		if (libver >= 28) {
			l += strncpy_escape_commas(fsnamearg+l,256-l,lfsopts.masterhost);
			if (l<255) {
				fsnamearg[l++]=':';
			}
			l += strncpy_escape_commas(fsnamearg+l,256-l,lfsopts.masterport);
			if (lfsopts.subfolder[0]!='/') {
				if (l<255) {
					fsnamearg[l++]='/';
				}
			}
			if (lfsopts.subfolder[0]!='/' && lfsopts.subfolder[1]!=0) {
				l += strncpy_escape_commas(fsnamearg+l,256-l,lfsopts.subfolder);
			}
			if (l>255) {
				l=255;
			}
			fsnamearg[l]=0;
		} else {
			l += strncpy_remove_commas(fsnamearg+l,256-l,lfsopts.masterhost);
			if (l<255) {
				fsnamearg[l++]=':';
			}
			l += strncpy_remove_commas(fsnamearg+l,256-l,lfsopts.masterport);
			if (lfsopts.subfolder[0]!='/') {
				if (l<255) {
					fsnamearg[l++]='/';
				}
			}
			if (lfsopts.subfolder[0]!='/' && lfsopts.subfolder[1]!=0) {
				l += strncpy_remove_commas(fsnamearg+l,256-l,lfsopts.subfolder);
			}
			if (l>255) {
				l=255;
			}
			fsnamearg[l]=0;
		}
	} else {
#else
		l = snprintf(fsnamearg,256,"-ofsname=lfs%s#",(lfsopts.meta)?"meta":"");
		l += strncpy_remove_commas(fsnamearg+l,256-l,lfsopts.masterhost);
		if (l<255) {
			fsnamearg[l++]=':';
		}
		l += strncpy_remove_commas(fsnamearg+l,256-l,lfsopts.masterport);
		if (lfsopts.subfolder[0]!='/') {
			if (l<255) {
				fsnamearg[l++]='/';
			}
		}
		if (lfsopts.subfolder[0]!='/' && lfsopts.subfolder[1]!=0) {
			l += strncpy_remove_commas(fsnamearg+l,256-l,lfsopts.subfolder);
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

int main(int argc, char *argv[]) {
	int res;
	int mt,fg;
	char *mountpoint;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_args defaultargs = FUSE_ARGS_INIT(0, NULL);

	strerr_init();
	mycrc32_init();

   init_fuse_lowlevel_ops();

	lfsopts.masterhost = NULL;
	lfsopts.masterport = NULL;
	lfsopts.bindhost = NULL;
	lfsopts.subfolder = NULL;
	lfsopts.password = NULL;
	lfsopts.md5pass = NULL;
	lfsopts.nofile = 0;
	lfsopts.nice = -19;
#ifdef LFS_USE_MEMLOCK
	lfsopts.memlock = 0;
#endif
	lfsopts.nostdmountoptions = 0;
	lfsopts.meta = 0;
	lfsopts.debug = 0;
	lfsopts.delayedinit = 0;
	lfsopts.acl = 0;
#ifdef __linux__
	lfsopts.mkdircopysgid = 1;
#else
	lfsopts.mkdircopysgid = 0;
#endif
	lfsopts.sugidclearmodestr = NULL;
	lfsopts.donotrememberpassword = 0;
	lfsopts.cachefiles = 0;
	lfsopts.cachemode = NULL;
	lfsopts.writecachesize = 0;
	lfsopts.aclcachesize = 1000;
	lfsopts.ioretries = 30;
	lfsopts.passwordask = 0;
	lfsopts.attrcacheto = 1.0;
	lfsopts.entrycacheto = 0.0;
	lfsopts.direntrycacheto = 1.0;
	lfsopts.aclcacheto = 1.0;
	lfsopts.reportreservedperiod = 60;

	custom_cfg = 0;

	fuse_opt_add_arg(&defaultargs,"fakeappname");

	if (fuse_opt_parse(&args, &defaultargs, lfs_opts_stage1, lfs_opt_proc_stage1)<0) {
		exit(1);
	}

	if (custom_cfg==0) {
		int cfgfd;
		char *cfgfile;

		cfgfile=strdup(ETC_PATH "/lfs/lfsmount.cfg");
		if ((cfgfd = open(cfgfile,O_RDONLY))<0 && errno==ENOENT) {
			free(cfgfile);
			cfgfile=strdup(ETC_PATH "/lfsmount.cfg");
			if ((cfgfd = open(cfgfile,O_RDONLY))>=0) {
				fprintf(stderr,"default sysconf path has changed - please move lfsmount.cfg from " ETC_PATH "/ to " ETC_PATH "/lfs/\n");
			}
		}
		if (cfgfd>=0) {
			close(cfgfd);
		}
		lfs_opt_parse_cfg_file(cfgfile,1,&defaultargs);
		free(cfgfile);
	}

	if (fuse_opt_parse(&defaultargs, &lfsopts, lfs_opts_stage2, lfs_opt_proc_stage2)<0) {
		exit(1);
	}

	if (fuse_opt_parse(&args, &lfsopts, lfs_opts_stage2, lfs_opt_proc_stage2)<0) {
		exit(1);
	}

	if (lfsopts.cachemode!=NULL && lfsopts.cachefiles) {
		fprintf(stderr,"lfscachemode and lfscachefiles options are exclusive - use only lfscachemode\nsee: %s -h for help\n",argv[0]);
		return 1;
	}

	if (lfsopts.cachemode==NULL) {
		lfsopts.keepcache=(lfsopts.cachefiles)?1:0;
	} else if (strcasecmp(lfsopts.cachemode,"AUTO")==0) {
		lfsopts.keepcache=0;
	} else if (strcasecmp(lfsopts.cachemode,"YES")==0 || strcasecmp(lfsopts.cachemode,"ALWAYS")==0) {
		lfsopts.keepcache=1;
	} else if (strcasecmp(lfsopts.cachemode,"NO")==0 || strcasecmp(lfsopts.cachemode,"NONE")==0 || strcasecmp(lfsopts.cachemode,"NEVER")==0) {
		lfsopts.keepcache=2;
	} else {
		fprintf(stderr,"unrecognized cachemode option\nsee: %s -h for help\n",argv[0]);
		return 1;
	}
	if (lfsopts.sugidclearmodestr==NULL) {
#if defined(DEFAULT_SUGID_CLEAR_MODE_EXT)
		lfsopts.sugidclearmode = SugidClearMode::kExt;
#elif defined(DEFAULT_SUGID_CLEAR_MODE_BSD)
		lfsopts.sugidclearmode = SugidClearMode::kBsd;
#elif defined(DEFAULT_SUGID_CLEAR_MODE_OSX)
		lfsopts.sugidclearmode = SugidClearMode::kOsx;
#else
		lfsopts.sugidclearmode = SugidClearMode::kNever;
#endif
	} else if (strcasecmp(lfsopts.sugidclearmodestr,"NEVER")==0) {
		lfsopts.sugidclearmode = SugidClearMode::kNever;
	} else if (strcasecmp(lfsopts.sugidclearmodestr,"ALWAYS")==0) {
		lfsopts.sugidclearmode = SugidClearMode::kAlways;
	} else if (strcasecmp(lfsopts.sugidclearmodestr,"OSX")==0) {
		lfsopts.sugidclearmode = SugidClearMode::kOsx;
	} else if (strcasecmp(lfsopts.sugidclearmodestr,"BSD")==0) {
		lfsopts.sugidclearmode = SugidClearMode::kBsd;
	} else if (strcasecmp(lfsopts.sugidclearmodestr,"EXT")==0) {
		lfsopts.sugidclearmode = SugidClearMode::kExt;
	} else if (strcasecmp(lfsopts.sugidclearmodestr,"XFS")==0) {
		lfsopts.sugidclearmode = SugidClearMode::kXfs;
	} else {
		fprintf(stderr,"unrecognized sugidclearmode option\nsee: %s -h for help\n",argv[0]);
		return 1;
	}
	if (lfsopts.masterhost==NULL) {
		lfsopts.masterhost = strdup("lfsmaster");
	}
	if (lfsopts.masterport==NULL) {
		lfsopts.masterport = strdup("9421");
	}
	if (lfsopts.subfolder==NULL) {
		lfsopts.subfolder = strdup("/");
	}
	if (lfsopts.nofile==0) {
		lfsopts.nofile=100000;
	}
	if (lfsopts.writecachesize==0) {
		lfsopts.writecachesize=128;
	}
	if (lfsopts.writecachesize<16) {
		fprintf(stderr,"write cache size too low (%u MiB) - increased to 16 MiB\n",lfsopts.writecachesize);
		lfsopts.writecachesize=16;
	}
	if (lfsopts.writecachesize>2048) {
		fprintf(stderr,"write cache size too big (%u MiB) - decreased to 2048 MiB\n",lfsopts.writecachesize);
		lfsopts.writecachesize=2048;
	}
	if (lfsopts.aclcachesize > 1000 * 1000) {
		fprintf(stderr,"acl cache size too big (%u) - decreased to 1000000\n",lfsopts.aclcachesize);
		lfsopts.aclcachesize=1000 * 1000;
	}

	if (lfsopts.nostdmountoptions==0) {
		fuse_opt_add_arg(&args, "-o" DEFAULT_OPTIONS);
	}


	make_fsname(&args);

	if (fuse_parse_cmdline(&args,&mountpoint,&mt,&fg)<0) {
		fprintf(stderr,"see: %s -h for help\n",argv[0]);
		return 1;
	}

	if (!mountpoint) {
		if (defaultmountpoint) {
			mountpoint = defaultmountpoint;
		} else {
			fprintf(stderr,"no mount point\nsee: %s -h for help\n",argv[0]);
			return 1;
		}
	}

	res = mainloop(&args,mountpoint,mt,fg);
	fuse_opt_free_args(&args);
	fuse_opt_free_args(&defaultargs);
	free(lfsopts.masterhost);
	free(lfsopts.masterport);
	if (lfsopts.bindhost) {
		free(lfsopts.bindhost);
	}
	free(lfsopts.subfolder);
	if (defaultmountpoint && defaultmountpoint != mountpoint) {
		free(defaultmountpoint);
	}
	if (lfsopts.iolimits) {
		free(lfsopts.iolimits);
	}
	free(mountpoint);
	stats_term();
	strerr_term();
	return res;
}
