/*
   Copyright 2013-2018 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "mount/fuse/mount_config.h"
#include "mount/sugid_clear_mode_string.h"

#include <stdio.h>
#include <stdlib.h>
#include <fuse.h>
#include <fuse_lowlevel.h>

#define MFS_OPT(t, p, v) { t, offsetof(struct mfsopts_, p), v }

mfsopts_ gMountOptions;

int gCustomCfg = 0;
char *gDefaultMountpoint = NULL;

struct fuse_opt gMfsOptsStage1[] = {
	FUSE_OPT_KEY("mfscfgfile=",    KEY_CFGFILE),
	FUSE_OPT_KEY("-c ",            KEY_CFGFILE),
	FUSE_OPT_END
};

struct fuse_opt gMfsOptsStage2[] = {
	MFS_OPT("mfsmaster=%s", masterhost, 0),
	MFS_OPT("mfsport=%s", masterport, 0),
	MFS_OPT("mfsbind=%s", bindhost, 0),
	MFS_OPT("mfssubfolder=%s", subfolder, 0),
	MFS_OPT("mfspassword=%s", password, 0),
	MFS_OPT("askpassword", passwordask, 1),
	MFS_OPT("mfsmd5pass=%s", md5pass, 0),
	MFS_OPT("mfsrlimitnofile=%u", nofile, 0),
	MFS_OPT("mfsnice=%d", nice, 0),
#ifdef MFS_USE_MEMLOCK
	MFS_OPT("mfsmemlock", memlock, 1),
#endif
	MFS_OPT("mfswritecachesize=%u", writecachesize, 0),
	MFS_OPT("mfsaclcachesize=%u", aclcachesize, 0),
	MFS_OPT("mfscacheperinodepercentage=%u", cachePerInodePercentage, 0),
	MFS_OPT("mfswriteworkers=%u", writeworkers, 0),
	MFS_OPT("mfsioretries=%u", ioretries, 0),
	MFS_OPT("mfswritewindowsize=%u", writewindowsize, 0),
	MFS_OPT("mfsdebug", debug, 1),
	MFS_OPT("mfsmeta", meta, 1),
	MFS_OPT("mfsdelayedinit", delayedinit, 1),
	MFS_OPT("mfsacl", acl, 1),
	MFS_OPT("mfsrwlock=%d", rwlock, 0),
	MFS_OPT("mfsdonotrememberpassword", donotrememberpassword, 1),
	MFS_OPT("mfscachefiles", cachefiles, 1),
	MFS_OPT("mfscachemode=%s", cachemode, 0),
	MFS_OPT("mfsmkdircopysgid=%u", mkdircopysgid, 0),
	MFS_OPT("mfssugidclearmode=%s", sugidclearmodestr, 0),
	MFS_OPT("mfsattrcacheto=%lf", attrcacheto, 0),
	MFS_OPT("mfsentrycacheto=%lf", entrycacheto, 0),
	MFS_OPT("mfsdirentrycacheto=%lf", direntrycacheto, 0),
	MFS_OPT("mfsaclcacheto=%lf", aclcacheto, 0),
	MFS_OPT("mfsreportreservedperiod=%u", reportreservedperiod, 0),
	MFS_OPT("mfsiolimits=%s", iolimits, 0),
	MFS_OPT("mfschunkserverrtt=%d", chunkserverrtt, 0),
	MFS_OPT("mfschunkserverconnectreadto=%d", chunkserverconnectreadto, 0),
	MFS_OPT("mfschunkserverwavereadto=%d", chunkserverwavereadto, 0),
	MFS_OPT("mfschunkservertotalreadto=%d", chunkservertotalreadto, 0),
	MFS_OPT("cacheexpirationtime=%d", cacheexpirationtime, 0),
	MFS_OPT("readaheadmaxwindowsize=%d", readaheadmaxwindowsize, 4096),
	MFS_OPT("mfsprefetchxorstripes", prefetchxorstripes, 1),
	MFS_OPT("mfschunkserverwriteto=%d", chunkserverwriteto, 0),
	MFS_OPT("symlinkcachetimeout=%d", symlinkcachetimeout, 3600),
	MFS_OPT("bandwidthoveruse=%lf", bandwidthoveruse, 1),
	MFS_OPT("mfsdirentrycachesize=%u", direntrycachesize, 0),
	MFS_OPT("nostdmountoptions", nostdmountoptions, 1),

#if FUSE_VERSION >= 26
	MFS_OPT("enablefilelocks=%u", filelocks, 0),
#endif
#if FUSE_VERSION >= 30
	MFS_OPT("nonempty", nonemptymount, 1),
#endif

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
#if FUSE_VERSION >= 30
	FUSE_OPT_KEY("--nonempty",     KEY_NONEMPTY),
#endif
#if FUSE_VERSION < 30
	FUSE_OPT_KEY("-V",             KEY_VERSION),
	FUSE_OPT_KEY("--version",      KEY_VERSION),
	FUSE_OPT_KEY("-h",             KEY_HELP),
	FUSE_OPT_KEY("--help",         KEY_HELP),
#endif
	FUSE_OPT_END
};

void usage(const char *progname) {
	printf(
"usage: %s  [HOST[/PORT]:[PATH]] [options] mountpoint\n"
"\n", progname);

	printf("general options:\n");
#if FUSE_VERSION >= 30
	fuse_cmdline_help();
#else
printf(
"    -o opt,[opt...]         mount options\n"
"    -h   --help             print help\n"
"    -V   --version          print version\n"
"\n");
#endif

	printf(
"MFS options:\n"
"    -c CFGFILE                  equivalent to '-o mfscfgfile=CFGFILE'\n"
"    -m   --meta                 equivalent to '-o mfsmeta'\n"
"    -H HOST                     equivalent to '-o mfsmaster=HOST'\n"
"    -P PORT                     equivalent to '-o mfsport=PORT'\n"
"    -B IP                       equivalent to '-o mfsbind=IP'\n"
"    -S PATH                     equivalent to '-o mfssubfolder=PATH'\n"
"    -p   --password             similar to '-o mfspassword=PASSWORD', but "
				"show prompt and ask user for password\n"
"    -n   --nostdopts            do not add standard LizardFS mount options: "
"'-o " DEFAULT_OPTIONS ",fsname=MFS'\n"
"    --nonempty                  allow mounts over non-empty file/dir\n"
"    -o nostdmountoptions        equivalent of --nostdopts for /etc/fstab\n"
"    -o mfscfgfile=CFGFILE       load some mount options from external file "
				"(if not specified then use default file: "
				ETC_PATH "/mfsmount.cfg)\n"
"    -o mfsdebug                 print some debugging information\n"
"    -o mfsmeta                  mount meta filesystem (trash etc.)\n"
"    -o mfsdelayedinit           connection with master is done in background "
				"- with this option mount can be run without "
				"network (good for being run from fstab/init "
				"scripts etc.)\n"
"    -o mfsacl                   DEPRECATED, used to enable/disable ACL "
				"support, ignored now\n"
"    -o mfsrwlock=0|1            when set to 1, parallel reads from the same "
				"descriptor are performed (default: %d)\n"
"    -o mfsmkdircopysgid=N       sgid bit should be copied during mkdir "
				"operation (default: %d)\n"
"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: %s)\n"
"    -o mfscachemode=CMODE       set cache mode (see below ; default: AUTO)\n"
"    -o mfscachefiles            (deprecated) equivalent to '-o mfscachemode=YES'\n"
"    -o mfsattrcacheto=SEC       set attributes cache timeout in seconds "
				"(default: %.2f)\n"
"    -o mfsentrycacheto=SEC      set file entry cache timeout in seconds "
				"(default: %.2f)\n"
"    -o mfsdirentrycacheto=SEC   set directory entry cache timeout in seconds "
				"(default: %.2f)\n"
"    -o mfsdirentrycachesize=N   define directory entry cache size in number "
				"of entries (default: %u)\n"
"    -o mfsaclcacheto=SEC        set ACL cache timeout in seconds (default: %.2f)\n"
"    -o mfsreportreservedperiod=SEC  set reporting reserved inodes interval in "
				"seconds (default: %u)\n"
"    -o mfschunkserverrtt=MSEC   set timeout after which SYN packet is "
				"considered lost during the first retry of "
				"connecting a chunkserver (default: %u)\n"
"    -o mfschunkserverconnectreadto=MSEC  set timeout for connecting with "
				"chunkservers during read operation in "
				"milliseconds (default: %u)\n"
"    -o mfschunkserverwavereadto=MSEC  set timeout for executing each wave "
				"of a read operation in milliseconds (default: %u)\n"
"    -o mfschunkservertotalreadto=MSEC  set timeout for the whole "
				"communication with chunkservers during a "
				"read operation in milliseconds (default: %u)\n"
"    -o cacheexpirationtime=MSEC  set timeout for read cache entries to be "
				"considered valid in milliseconds (0 disables "
				"cache) (default: %u)\n"
"    -o readaheadmaxwindowsize=KB  set max value of readahead window per single "
				"descriptor in kibibytes (default: %u)\n"
"    -o mfsprefetchxorstripes    prefetch full xor stripe on every first read "
				"of a xor chunk\n"
"    -o mfschunkserverwriteto=MSEC  set chunkserver response timeout during "
				"write operation in milliseconds (default: %u)\n"
"    -o mfsnice=N                on startup mfsmount tries to change his "
				"'nice' value (default: -19)\n"
#ifdef MFS_USE_MEMLOCK
"    -o mfsmemlock               try to lock memory\n"
#endif
"    -o mfswritecachesize=N      define size of write cache in MiB (default: %u)\n"
"    -o mfsaclcachesize=N        define ACL cache size in number of entries "
				"(0: no cache; default: %u)\n"
"    -o mfscacheperinodepercentage  define what part of the write cache non "
				"occupied by other inodes can a single inode "
				"occupy (in %%, default: %u)\n"
"    -o mfswriteworkers=N        define number of write workers (default: %u)\n"
"    -o mfsioretries=N           define number of retries before I/O error is "
				"returned (default: %u)\n"
"    -o mfswritewindowsize=N     define write window size (in blocks) for "
				"each chunk (default: %u)\n"
"    -o mfsmaster=HOST           define mfsmaster location (default: mfsmaster)\n"
"    -o mfsport=PORT             define mfsmaster port number (default: 9421)\n"
"    -o mfsbind=IP               define source ip address for connections "
				"(default: NOT DEFINED - chosen automatically "
				"by OS)\n"
"    -o mfssubfolder=PATH        define subfolder to mount as root (default: %s)\n"
"    -o mfspassword=PASSWORD     authenticate to mfsmaster with password\n"
"    -o mfsmd5pass=MD5           authenticate to mfsmaster using directly "
				"given md5 (only if mfspassword is not defined)\n"
"    -o askpassword              show prompt and ask user for password\n"
"    -o mfsdonotrememberpassword do not remember password in memory - more "
				"secure, but when session is lost then new "
				"session is created without password\n"
"    -o mfsiolimits=FILE         define I/O limits configuration file\n"
"    -o symlinkcachetimeout=N    define timeout of symlink cache in seconds "
				"(default: %u)\n"
"    -o bandwidthoveruse=N       define ratio of allowed bandwidth overuse "
				"when fetching data (default: %.2f)\n"
#if FUSE_VERSION >= 26
"    -o enablefilelocks=0|1      enables/disables global file locking "
				"(disabled by default)\n"
#endif
#if FUSE_VERSION >= 30
"    -o nonempty                 allow mounts over non-empty file/dir\n"
#endif
"\n",
		LizardClient::FsInitParams::kDefaultUseRwLock,
		LizardClient::FsInitParams::kDefaultMkdirCopySgid,
		sugidClearModeString(LizardClient::FsInitParams::kDefaultSugidClearMode),
		LizardClient::FsInitParams::kDefaultAttrCacheTimeout,
		LizardClient::FsInitParams::kDefaultEntryCacheTimeout,
		LizardClient::FsInitParams::kDefaultDirentryCacheTimeout,
		LizardClient::FsInitParams::kDefaultDirentryCacheSize,
		LizardClient::FsInitParams::kDefaultAclCacheTimeout,
		LizardClient::FsInitParams::kDefaultReportReservedPeriod,
		LizardClient::FsInitParams::kDefaultRoundTime,
		LizardClient::FsInitParams::kDefaultChunkserverReadTo,
		LizardClient::FsInitParams::kDefaultChunkserverWaveReadTo,
		LizardClient::FsInitParams::kDefaultChunkserverTotalReadTo,
		LizardClient::FsInitParams::kDefaultCacheExpirationTime,
		LizardClient::FsInitParams::kDefaultReadaheadMaxWindowSize,
		LizardClient::FsInitParams::kDefaultChunkserverWriteTo,
		LizardClient::FsInitParams::kDefaultWriteCacheSize,
		LizardClient::FsInitParams::kDefaultAclCacheSize,
		LizardClient::FsInitParams::kDefaultCachePerInodePercentage,
		LizardClient::FsInitParams::kDefaultWriteWorkers,
		LizardClient::FsInitParams::kDefaultIoRetries,
		LizardClient::FsInitParams::kDefaultWriteWindowSize,
		LizardClient::FsInitParams::kDefaultSubfolder,
		LizardClient::FsInitParams::kDefaultSymlinkCacheTimeout,
		LizardClient::FsInitParams::kDefaultBandwidthOveruse
	);
	printf(
"CMODE can be set to:\n"
"    NO,NONE or NEVER            never allow files data to be kept in cache "
				"(safest but can reduce efficiency)\n"
"    YES or ALWAYS               always allow files data to be kept in cache "
				"(dangerous)\n"
"    AUTO                        file cache is managed by mfsmaster "
				"automatically (should be very safe and "
				"efficient)\n"
"\n");
	printf(
"SMODE can be set to:\n"
"    NEVER                       LizardFS will not change suid and sgid bit "
				"on chown\n"
"    ALWAYS                      clear suid and sgid on every chown - safest "
				"operation\n"
"    OSX                         standard behavior in OS X and Solaris (chown "
				"made by unprivileged user clear suid and sgid)\n"
"    BSD                         standard behavior in *BSD systems (like in "
				"OSX, but only when something is really changed)\n"
"    EXT                         standard behavior in most file systems on "
				"Linux (directories not changed, others: suid "
				"cleared always, sgid only when group exec "
				"bit is set)\n"
"    XFS                         standard behavior in XFS on Linux (like EXT "
				"but directories are changed by unprivileged "
				"users)\n"
"SMODE extra info:\n"
"    btrfs,ext2,ext3,ext4,hfs[+],jfs,ntfs and reiserfs on Linux work as 'EXT'.\n"
"    Only xfs on Linux works a little different. Beware that there is a strange\n"
"    operation - chown(-1,-1) which is usually converted by a kernel into something\n"
"    like 'chmod ug-s', and therefore can't be controlled by MFS as 'chown'\n"
"\n");

#if FUSE_VERSION >= 30
	printf("\nFUSE options:\n");
	fuse_lowlevel_help();
	printf(
	"    -o max_write=N\n"
	"    -o max_readahead=N\n"
	"    -o max_background=N\n"
	"    -o congestion_threshold=N\n"
	"    -o sync_read \n"
	"    -o async_read \n"
	"    -o atomic_o_trunc \n"
	"    -o no_remote_lock \n"
	"    -o no_remote_flock \n"
	"    -o no_remote_posix_lock \n"
	"    -o splice_write \n"
	"    -o no_splice_write \n"
	"    -o splice_move \n"
	"    -o no_splice_move \n"
	"    -o splice_read \n"
	"    -o no_splice_read \n"
	"    -o auto_inval_data \n"
	"    -o no_auto_inval_data \n"
	"    -o readdirplus=(yes,no,auto)\n"
	"    -o async_dio \n"
	"    -o no_async_dio \n"
	"    -o writeback_cache \n"
	"    -o no_writeback_cache \n"
	"    -o time_gran=N\n"
	);
#endif
}

void mfs_opt_parse_cfg_file(const char *filename,int optional,struct fuse_args *outargs) {
	FILE *fd;
	constexpr size_t N = 1000;
	char lbuff[N],*p;

	fd = fopen(filename, "r");
	if (!fd) {
		if (!optional) {
			fprintf(stderr,"can't open cfg file: %s\n",filename);
			abort();
		}
		return;
	}
	gCustomCfg = 1;
	while (fgets(lbuff, N - 1, fd)) {
		if (lbuff[0] == '#' || lbuff[0] == ';')
			continue;

		lbuff[N - 1] = 0;

		for (p = lbuff; *p; p++) {
			if (*p == '\r' || *p == '\n') {
				*p = 0;
				break;
			}
		}

		p--;

		while (p >= lbuff && (*p == ' ' || *p == '\t')) {
			*p = 0;
			p--;
		}

		p = lbuff;

		while (*p == ' ' || *p == '\t') {
			p++;
		}

		if (*p) {
			if (*p == '-') {
				fuse_opt_add_arg(outargs,p);
			} else if (*p == '/') {
				if (gDefaultMountpoint)
					free(gDefaultMountpoint);
				gDefaultMountpoint = strdup(p);
			} else {
				fuse_opt_add_arg(outargs,"-o");
				fuse_opt_add_arg(outargs,p);
			}
		}
	}
	fclose(fd);
}

// Function for FUSE: has to have these arguments
int mfs_opt_proc_stage1(struct fuse_args *defargs, const char *arg, int key) {
	const char *mfscfgfile_opt = "mfscfgfile=";
	const int n = strlen(mfscfgfile_opt);

	if (key == KEY_CFGFILE) {
		if (!strncmp(arg, mfscfgfile_opt, n))
			mfs_opt_parse_cfg_file(arg + n, 0, defargs);
		else if (!strncmp(arg, "-c", 2))
			mfs_opt_parse_cfg_file(arg + 2, 0, defargs);

		return 0;
	}
	return 1;
}

int mfs_opt_proc_stage1(void *data, const char *arg, int key, struct fuse_args *outargs) {
	(void)outargs; // remove unused argument warning
	return mfs_opt_proc_stage1((struct fuse_args*)data, arg, key);
}

// Function for FUSE: has to have these arguments
// return value:
//   0 - discard this arg
//   1 - keep this arg for future processing
int mfs_opt_proc_stage2(void *data, const char *arg, int key, struct fuse_args *outargs) {
	(void)data; // remove unused argument warning
	(void)outargs;

	switch (key) {
	case FUSE_OPT_KEY_OPT:
		return 1;
	case FUSE_OPT_KEY_NONOPT:
		return 1;
	case KEY_HOST:
		if (gMountOptions.masterhost)
			free(gMountOptions.masterhost);
		gMountOptions.masterhost = strdup(arg + 2);
		return 0;
	case KEY_PORT:
		if (gMountOptions.masterport)
			free(gMountOptions.masterport);
		gMountOptions.masterport = strdup(arg + 2);
		return 0;
	case KEY_BIND:
		if (gMountOptions.bindhost)
			free(gMountOptions.bindhost);
		gMountOptions.bindhost = strdup(arg + 2);
		return 0;
	case KEY_PATH:
		if (gMountOptions.subfolder)
			free(gMountOptions.subfolder);
		gMountOptions.subfolder = strdup(arg + 2);
		return 0;
	case KEY_PASSWORDASK:
		gMountOptions.passwordask = 1;
		return 0;
	case KEY_META:
		gMountOptions.meta = 1;
		return 0;
	case KEY_NOSTDMOUNTOPTIONS:
		gMountOptions.nostdmountoptions = 1;
		return 0;
#if FUSE_VERSION >= 30
	case KEY_NONEMPTY:
		gMountOptions.nonemptymount = 1;
		return 0;
#endif
#if FUSE_VERSION < 30
	case KEY_VERSION:
		printf("LizardFS version %s\n", LIZARDFS_PACKAGE_VERSION);
		{
			struct fuse_args helpargs = FUSE_ARGS_INIT(0, NULL);
			fuse_opt_add_arg(&helpargs, outargs->argv[0]);
			fuse_opt_add_arg(&helpargs, "--version");
			fuse_parse_cmdline(&helpargs, NULL, NULL, NULL);
			fuse_unmount(NULL, fuse_mount(NULL, &helpargs));
		}
		exit(0);
	case KEY_HELP:
		usage(outargs->argv[0]);
		{
			struct fuse_args helpargs = FUSE_ARGS_INIT(0, NULL);
			fuse_opt_add_arg(&helpargs,outargs->argv[0]);
			fuse_opt_add_arg(&helpargs, "-ho");
			fuse_parse_cmdline(&helpargs, NULL, NULL, NULL);
			fuse_unmount(NULL, fuse_mount(NULL, &helpargs));
		}
		exit(0);
#endif
	default:
		fprintf(stderr, "internal error\n");
		abort();
	}
}
