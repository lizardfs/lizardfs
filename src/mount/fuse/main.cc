/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2018
   Skytechnology sp. z o.o..

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

#include <dirent.h>
#include <errno.h>
#include <fstream>
#include <fuse.h>
#include <fuse_lowlevel.h>
#include <sys/types.h>

#include "common/crc.h"
#include "common/md5.h"
#include "common/mfserr.h"
#include "common/sockets.h"
#include "mount/fuse/daemonize.h"
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
#include "protocol/MFSCommunication.h"

#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)

static void mfs_fsinit(void *userdata, struct fuse_conn_info *conn);

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

static void mfs_fsinit(void *userdata, struct fuse_conn_info *conn) {
	(void)userdata;
	(void)conn;

#if FUSE_VERSION >= 28
	conn->want |= FUSE_CAP_DONT_MASK;
#endif

#if FUSE_VERSION >= 30
	fuse_conn_info_opts *conn_opts = (fuse_conn_info_opts *)userdata;
	fuse_apply_conn_info_opts(conn_opts, conn);
	conn->want |= FUSE_CAP_POSIX_ACL;
	conn->want &= ~FUSE_CAP_ATOMIC_O_TRUNC;
#endif

	daemonize_return_status(0);
}

static bool setup_password(std::vector<uint8_t> &md5pass) {
	md5ctx ctx;

	if (gMountOptions.password) {
		md5pass.resize(16);
		md5_init(&ctx);
		md5_update(&ctx, (uint8_t *)(gMountOptions.password), strlen(gMountOptions.password));
		md5_final(md5pass.data(), &ctx);
		memset(gMountOptions.password, 0, strlen(gMountOptions.password));
	} else if (gMountOptions.md5pass) {
		int ret = md5_parse(md5pass, gMountOptions.md5pass);
		if (ret) {
			fprintf(stderr, "bad md5 definition (md5 should be given as 32 hex digits)\n");
			return false;
		}
		memset(gMountOptions.md5pass, 0, strlen(gMountOptions.md5pass));
	}

	return true;
}

#if FUSE_VERSION >= 30
int fuse_mnt_check_empty(const char *mnt, mode_t rootmode, off_t rootsize) {
	int isempty = 1;

	if (S_ISDIR(rootmode)) {
		struct dirent *ent;
		DIR *dp = opendir(mnt);
		if (!dp) {
			return -1;
		}
		while ((ent = readdir(dp))) {
			if (strncmp(ent->d_name, ".", 1) &&
			    strncmp(ent->d_name, "..", 2)) {
				isempty = 0;
				break;
			}
		}
		closedir(dp);
	} else if (rootsize) {
		isempty = 0;
	}

	if (!isempty)
		return -1;

	return 0;
}
#endif

#if FUSE_VERSION >= 30
static int mainloop(struct fuse_args *args, struct fuse_cmdline_opts *fuse_opts,
			struct fuse_conn_info_opts *conn_opts) try {
	const char *mountpoint = fuse_opts->mountpoint;
	bool multithread = !fuse_opts->singlethread;
	bool foreground = fuse_opts->foreground;
#else
static int mainloop(struct fuse_args *args, const char *mountpoint, bool multithread,
			bool foreground) try {
#endif

	if (!foreground) {
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY, LOG_DAEMON);
	} else {
#if defined(LOG_PERROR)
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY | LOG_PERROR, LOG_USER);
#else
		openlog(STR(APPNAME), LOG_PID | LOG_NDELAY, LOG_USER);
#endif
	}
	lzfs::add_log_syslog();
	if (!foreground)
		lzfs::add_log_stderr(lzfs::log_level::debug);

	struct rlimit rls;
	rls.rlim_cur = gMountOptions.nofile;
	rls.rlim_max = gMountOptions.nofile;
	setrlimit(RLIMIT_NOFILE, &rls);

	setpriority(PRIO_PROCESS, getpid(), gMountOptions.nice);
#ifdef MFS_USE_MEMLOCK
	if (gMountOptions.memlock) {
		rls.rlim_cur = RLIM_INFINITY;
		rls.rlim_max = RLIM_INFINITY;
		if (setrlimit(RLIMIT_MEMLOCK, &rls))
			gMountOptions.memlock = 0;
	}
#endif

#ifdef MFS_USE_MEMLOCK
	if (gMountOptions.memlock &&  !mlockall(MCL_CURRENT | MCL_FUTURE))
		lzfs_pretty_syslog(LOG_NOTICE, "process memory was "
				"successfully locked in RAM");
#endif

	std::vector<uint8_t> md5pass;
	if (!setup_password(md5pass)) {
		return 1;
	}
	LizardClient::FsInitParams params(gMountOptions.bindhost ?
			gMountOptions.bindhost : "", gMountOptions.masterhost,
			gMountOptions.masterport, mountpoint);
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

	if (!gMountOptions.meta) {
		LizardClient::fs_init(params);
	} else {
		masterproxy_init();
		symlink_cache_init();
		if (gMountOptions.delayedinit) {
			fs_init_master_connection(params);
		} else {
			if (fs_init_master_connection(params) < 0) {
				return 1;
			}
		}
		fs_init_threads(params.io_retries);
	}

#if FUSE_VERSION < 30
	struct fuse_chan *ch = fuse_mount(mountpoint, args);
	if (!ch) {
		fprintf(stderr, "error in fuse_mount\n");
		if (!gMountOptions.meta) {
			LizardClient::fs_term();
		} else {
			masterproxy_term();
			fs_term();
			symlink_cache_term();
		}
		return 1;
	}
#endif

#if FUSE_VERSION >= 30
	struct fuse_session *se;
	if (gMountOptions.meta) {
		mfs_meta_init(gMountOptions.debug, gMountOptions.entrycacheto, gMountOptions.attrcacheto);
		se = fuse_session_new(args, &mfs_meta_oper, sizeof(mfs_meta_oper), (void *)conn_opts);
	} else {
		se = fuse_session_new(args, &mfs_oper, sizeof(mfs_oper), (void *)conn_opts);
	}
	if (!se) {
		fprintf(stderr, "error in fuse_session_new\n");
#else
	struct fuse_session *se;
	if (gMountOptions.meta) {
		mfs_meta_init(gMountOptions.debug, gMountOptions.entrycacheto, gMountOptions.attrcacheto);
		se = fuse_lowlevel_new(args, &mfs_meta_oper, sizeof(mfs_meta_oper), NULL);
	} else {
		se = fuse_lowlevel_new(args, &mfs_oper, sizeof(mfs_oper), NULL);
	}
	if (!se) {
		fuse_unmount(mountpoint, ch);
		fprintf(stderr, "error in fuse_lowlevel_new\n");
#endif
		usleep(100000);  // time for print other error messages by FUSE
		if (!gMountOptions.meta) {
			LizardClient::fs_term();
		} else {
			masterproxy_term();
			fs_term();
			symlink_cache_term();
		}
		return 1;
	}

	if (fuse_set_signal_handlers(se)) {
		fprintf(stderr, "error in fuse_set_signal_handlers\n");
		fuse_session_destroy(se);
#if FUSE_VERSION < 30
		fuse_unmount(mountpoint, ch);
#endif
		if (!gMountOptions.meta) {
			LizardClient::fs_term();
		} else {
			masterproxy_term();
			fs_term();
			symlink_cache_term();
		}
		return 1;
	}

#if FUSE_VERSION >= 30
	if (fuse_session_mount(se, mountpoint)) {
		fprintf(stderr, "error in fuse_session_mount\n");
		fuse_remove_signal_handlers(se);
		fuse_session_destroy(se);
		if (!gMountOptions.meta) {
			LizardClient::fs_term();
		} else {
			masterproxy_term();
			fs_term();
			symlink_cache_term();
		}
		return 1;
	}
#else
	fuse_session_add_chan(se, ch);
#endif

	if (!gMountOptions.debug && !foreground) {
		setsid();
		setpgid(0, getpid());
		int nullfd = open("/dev/null", O_RDWR, 0);
		if (nullfd != -1) {
			(void)dup2(nullfd, 0);
			(void)dup2(nullfd, 1);
			(void)dup2(nullfd, 2);
			if (nullfd > 2)
				close(nullfd);
		}
	}

	int err;
	if (multithread) {
#if FUSE_VERSION >= 30
		err = fuse_session_loop_mt(se, fuse_opts->clone_fd);
#else
		err = fuse_session_loop_mt(se);
#endif
	} else {
		err = fuse_session_loop(se);
	}
	fuse_remove_signal_handlers(se);
#if FUSE_VERSION >= 30
	fuse_session_unmount(se);
#else
	fuse_session_remove_chan(ch);
#endif
	fuse_session_destroy(se);
#if FUSE_VERSION < 30
	fuse_unmount(mountpoint, ch);
#endif
	if (!gMountOptions.meta) {
		LizardClient::fs_term();
	} else {
		masterproxy_term();
		fs_term();
		symlink_cache_term();
	}
	return err ? 1 : 0;
} catch (...) {
	return 1;
}

static unsigned int strncpy_remove_commas(char *dstbuff, unsigned int dstsize, char *src) {
	char c;
	unsigned int l;
	l = 0;
	while ((c = *src++) && l + 1 < dstsize) {
		if (c != ',') {
			*dstbuff++ = c;
			l++;
		}
	}
	*dstbuff = 0;
	return l;
}

static unsigned int strncpy_escape_commas(char *dstbuff, unsigned int dstsize, char *src) {
	char c;
	unsigned int l;
	l = 0;
	while ((c = *src++) && l + 1 < dstsize) {
		if (c != ',' && c != '\\') {
			*dstbuff++ = c;
			l++;
		} else {
			if (l + 2 < dstsize) {
				*dstbuff++ = '\\';
				*dstbuff++ = c;
				l += 2;
			} else {
				*dstbuff = 0;
				return l;
			}
		}
	}
	*dstbuff = 0;
	return l;
}

static void make_fsname(struct fuse_args *args) {
	unsigned int l;
	char fsnamearg[256];
	int libver = fuse_version();
	unsigned int (*strncpy_commas)(char*, unsigned int, char*) = libver >= 28 ? strncpy_escape_commas : strncpy_remove_commas;
	const char *fmt = libver >= 27 ? "-osubtype=mfs%s,fsname=" : "-ofsname=mfs%s#";
	l = snprintf(fsnamearg, 256, fmt, (gMountOptions.meta) ? "meta" : "");
	l += strncpy_commas(fsnamearg + l, 256 - l, gMountOptions.masterhost);

	if (l < 255)
		fsnamearg[l++] = ':';

	l += strncpy_commas(fsnamearg + l, 256 - l, gMountOptions.masterport);

	if (gMountOptions.subfolder[0] != '/' && l < 255)
		fsnamearg[l++] = '/';

	if (gMountOptions.subfolder[0] != '/' && gMountOptions.subfolder[1] != 0)
		l += strncpy_commas(fsnamearg + l, 256 - l, gMountOptions.subfolder);

	if (l > 255)
		l = 255;

	fsnamearg[l] = 0;
	fuse_opt_insert_arg(args, 1, fsnamearg);
}

static int is_dns_char(char c) {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
	       (c >= '0' && c <= '9') || c == '-' || c == '.';
}

static size_t count_colons_in_str(const char *str, size_t len) {
	size_t colons_count = 0;

	for (size_t i = 0; i < len; i++)
		if (str[i] == ':')
			colons_count++;

	return colons_count;
}

/**
 * Find and parse arg that matches to HOST[:PORT]:[PATH] pattern.
 */
static int read_masterhost_if_present(struct fuse_args *args) {
	if (args->argc < 2)
		return 0;

	char *c;
	int optpos = 1;

	while (optpos < args->argc) {
		c = args->argv[optpos];

		if (!strncmp(c, "-o", 2))
			optpos += strlen(c) > 2 ? 1 : 2;
		else
			break;
	}

	if (optpos >= args->argc)
		return 0;

	size_t colons = count_colons_in_str(c, strlen(c));

	if (!colons)
		return 0;

	uint32_t hostlen = 0;

	while (is_dns_char(*c)) {
		c++;
		hostlen++;
	}

	if (!hostlen)
		return 0;

	uint32_t portlen = 0;
	char *portbegin = NULL;

	if (*c == ':' && colons > 1) {
		c++;
		portbegin = c;
		while (*c >= '0' && *c <= '9') {
			c++;
			portlen++;
		}
	}

	if (*c != ':')
		return 0;

	c++;

	if (*c)
		gMountOptions.subfolder = strdup(c);

	if (!(gMountOptions.masterhost = (char*)malloc(hostlen + 1)))
		return -1;
	strncpy(gMountOptions.masterhost, args->argv[optpos], hostlen);

	if (portbegin && portlen) {
		if (!(gMountOptions.masterport = (char*)malloc(portlen + 1)))
			return -1;
		strncpy(gMountOptions.masterport, portbegin, portlen);
	}

	for (int i = optpos + 1; i < args->argc; i++)
		args->argv[i - 1] = args->argv[i];

	args->argc--;

	return 0;
}

int main(int argc, char *argv[]) try {
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	struct fuse_args defaultargs = FUSE_ARGS_INIT(0, NULL);

	fuse_opt_add_arg(&defaultargs, "fakeappname");

	if (read_masterhost_if_present(&args))
		exit(1);

	if (fuse_opt_parse(&args, &defaultargs, gMfsOptsStage1, mfs_opt_proc_stage1))
		exit(1);

	if (!gCustomCfg)
		mfs_opt_parse_cfg_file(DEFAULT_MSFMOUNT_CONFIG_PATH, 1, &defaultargs);

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
#endif

	init_fuse_lowlevel_ops();

	if (gMountOptions.cachemode && gMountOptions.cachefiles) {
		fprintf(stderr,
			"mfscachemode and mfscachefiles options are exclusive "
			"- use only " "mfscachemode\nsee: %s -h for help\n",
		        argv[0]);
		return 1;
	}

	if (!gMountOptions.cachemode) {
		gMountOptions.keepcache = (gMountOptions.cachefiles) ? 1 : 0;
	} else if (!strcasecmp(gMountOptions.cachemode, "AUTO")) {
		gMountOptions.keepcache = 0;
	} else if (!strcasecmp(gMountOptions.cachemode, "YES") ||
	           !strcasecmp(gMountOptions.cachemode, "ALWAYS")) {
		gMountOptions.keepcache = 1;
	} else if (!strcasecmp(gMountOptions.cachemode, "NO") ||
	           !strcasecmp(gMountOptions.cachemode, "NONE") ||
	           !strcasecmp(gMountOptions.cachemode, "NEVER")) {
		gMountOptions.keepcache = 2;
		gMountOptions.cacheexpirationtime = 0;
	} else {
		fprintf(stderr, "unrecognized cachemode option\nsee: %s -h "
				"for help\n", argv[0]);
		return 1;
	}
	if (!gMountOptions.sugidclearmodestr) {
		gMountOptions.sugidclearmode = LizardClient::FsInitParams::kDefaultSugidClearMode;
	} else if (!strcasecmp(gMountOptions.sugidclearmodestr, "NEVER")) {
		gMountOptions.sugidclearmode = SugidClearMode::kNever;
	} else if (!strcasecmp(gMountOptions.sugidclearmodestr, "ALWAYS")) {
		gMountOptions.sugidclearmode = SugidClearMode::kAlways;
	} else if (!strcasecmp(gMountOptions.sugidclearmodestr, "OSX")) {
		gMountOptions.sugidclearmode = SugidClearMode::kOsx;
	} else if (!strcasecmp(gMountOptions.sugidclearmodestr, "BSD")) {
		gMountOptions.sugidclearmode = SugidClearMode::kBsd;
	} else if (!strcasecmp(gMountOptions.sugidclearmodestr, "EXT")) {
		gMountOptions.sugidclearmode = SugidClearMode::kExt;
	} else if (!strcasecmp(gMountOptions.sugidclearmodestr, "XFS")) {
		gMountOptions.sugidclearmode = SugidClearMode::kXfs;
	} else {
		fprintf(stderr, "unrecognized sugidclearmode option\nsee: %s "
				"-h for help\n", argv[0]);
		return 1;
	}

	if (!gMountOptions.masterhost)
		gMountOptions.masterhost = strdup(DEFAULT_MASTER_HOSTNAME);

	if (!gMountOptions.masterport)
		gMountOptions.masterport = strdup(DEFAULT_MASTER_PORT);

	if (!gMountOptions.subfolder)
		gMountOptions.subfolder = strdup(DEFAULT_MOUNTED_SUBFOLDER);

	if (!gMountOptions.nofile)
		gMountOptions.nofile = 100000;

	if (!gMountOptions.writecachesize)
		gMountOptions.writecachesize = 128;

	if (gMountOptions.cachePerInodePercentage < 1) {
		fprintf(stderr, "cache per inode percentage too low (%u %%) - "
				"increased to 1%%\n",
		        gMountOptions.cachePerInodePercentage);
		gMountOptions.cachePerInodePercentage = 1;
	}

	if (gMountOptions.cachePerInodePercentage > 100) {
		fprintf(stderr, "cache per inode percentage too big (%u %%) - "
				"decreased to 100%%\n",
		        gMountOptions.cachePerInodePercentage);
		gMountOptions.cachePerInodePercentage = 100;
	}

	if (gMountOptions.writecachesize < 16) {
		fprintf(stderr, "write cache size too low (%u MiB) - "
				"increased to 16 MiB\n",
		        gMountOptions.writecachesize);
		gMountOptions.writecachesize = 16;
	}

	if (gMountOptions.writecachesize > 1024 * 1024) {
		fprintf(stderr, "write cache size too big (%u MiB) - "
				"decreased to 1 TiB\n",
		        gMountOptions.writecachesize);
		gMountOptions.writecachesize = 1024 * 1024;
	}

	if (gMountOptions.writeworkers < 1) {
		fprintf(stderr, "no write workers - increasing number of "
				"workers to 1\n");
		gMountOptions.writeworkers = 1;
	}

	if (gMountOptions.writewindowsize < 1) {
		fprintf(stderr, "write window size is 0 - increasing to 1\n");
		gMountOptions.writewindowsize = 1;
	}

	if (!gMountOptions.nostdmountoptions)
		fuse_opt_add_arg(&args, "-o" DEFAULT_OPTIONS);

	if (gMountOptions.aclcachesize > 1000 * 1000) {
		fprintf(stderr, "acl cache size too big (%u) - decreased to "
				"1000000\n", gMountOptions.aclcachesize);
		gMountOptions.aclcachesize = 1000 * 1000;
	}

	if (gMountOptions.direntrycachesize > 10000000) {
		fprintf(stderr, "directory entry cache size too big (%u) - "
				"decreased to 10000000\n",
		        gMountOptions.direntrycachesize);
		gMountOptions.direntrycachesize = 10000000;
	}

	make_fsname(&args);

#if FUSE_VERSION >= 30
	struct fuse_cmdline_opts fuse_opts;
	if (fuse_parse_cmdline(&args, &fuse_opts)) {
		fprintf(stderr, "see: %s -h for help\n", argv[0]);
		return 1;
	}

	if (fuse_opts.show_help) {
		usage(argv[0]);
		return 0;
	}

	if (fuse_opts.show_version) {
		printf("LizardFS version %s\n", LIZARDFS_PACKAGE_VERSION);
		printf("FUSE library version: %s\n", fuse_pkgversion());
		fuse_lowlevel_version();
		return 0;
	}
#else
	int multithread, foreground;
	char *mountpoint;

	if (fuse_parse_cmdline(&args, &mountpoint, &multithread, &foreground)) {
		fprintf(stderr, "see: %s -h for help\n", argv[0]);
		return 1;
	}
#endif

	if (gMountOptions.passwordask && !gMountOptions.password && !gMountOptions.md5pass)
		gMountOptions.password = getpass("LizardFS Password:");

#if FUSE_VERSION >= 30
	if (!fuse_opts.mountpoint) {
		if (gDefaultMountpoint) {
			fuse_opts.mountpoint = gDefaultMountpoint;
#else
	if (!mountpoint) {
		if (gDefaultMountpoint) {
			mountpoint = gDefaultMountpoint;
#endif
		} else {
			fprintf(stderr, "no mount point\nsee: %s -h for help\n", argv[0]);
			return 1;
		}
	}

	int res;
#if FUSE_VERSION >= 30
	struct stat stbuf;
	res = stat(fuse_opts.mountpoint, &stbuf);
	if (res) {
		fprintf(stderr, "failed to access mountpoint %s: %s\n",
			fuse_opts.mountpoint, strerror(errno));
		return 1;
	}
	if (!gMountOptions.nonemptymount) {
		if (fuse_mnt_check_empty(fuse_opts.mountpoint, stbuf.st_mode,
					 stbuf.st_size)) {
			return 1;
		}
	}

	if (!fuse_opts.foreground)
		res = daemonize_and_wait(std::bind(&mainloop, &args, &fuse_opts, conn_opts));
	else
		res = mainloop(&args, &fuse_opts, conn_opts);
#else
	if (!foreground)
		res = daemonize_and_wait(std::bind(&mainloop, &args, mountpoint, multithread, foreground));
	else
		res = mainloop(&args, mountpoint, multithread, foreground);
#endif

	fuse_opt_free_args(&args);
	fuse_opt_free_args(&defaultargs);
	free(gMountOptions.masterhost);
	free(gMountOptions.masterport);
	if (gMountOptions.bindhost)
		free(gMountOptions.bindhost);
	free(gMountOptions.subfolder);
	if (gMountOptions.iolimits)
		free(gMountOptions.iolimits);
#if FUSE_VERSION >= 30
	if (gDefaultMountpoint && gDefaultMountpoint != fuse_opts.mountpoint)
		free(gDefaultMountpoint);
	free(fuse_opts.mountpoint);
	free(conn_opts);
#else
	if (gDefaultMountpoint && gDefaultMountpoint != mountpoint)
		free(gDefaultMountpoint);
	free(mountpoint);
#endif
	stats_term();
	return res;
} catch (std::bad_alloc& ex) {
	mabort("run out of memory");
}
