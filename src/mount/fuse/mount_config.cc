/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

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

#include <stdio.h>
#include <stdlib.h>
#include <fuse/fuse.h>

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

#if FUSE_VERSION >= 26
	MFS_OPT("enablefilelocks=%u", filelocks, 0),
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
	FUSE_OPT_KEY("-V",             KEY_VERSION),
	FUSE_OPT_KEY("--version",      KEY_VERSION),
	FUSE_OPT_KEY("-h",             KEY_HELP),
	FUSE_OPT_KEY("--help",         KEY_HELP),
	FUSE_OPT_END
};

void usage(const char *progname) {
#define STR_AUX(x) #x
#define STR(x) STR_AUX(x)
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
"MFS options:\n"
"    -c CFGFILE                  equivalent to '-o mfscfgfile=CFGFILE'\n"
"    -m   --meta                 equivalent to '-o mfsmeta'\n"
"    -H HOST                     equivalent to '-o mfsmaster=HOST'\n"
"    -P PORT                     equivalent to '-o mfsport=PORT'\n"
"    -B IP                       equivalent to '-o mfsbind=IP'\n"
"    -S PATH                     equivalent to '-o mfssubfolder=PATH'\n"
"    -p   --password             similar to '-o mfspassword=PASSWORD', but show prompt and ask user for password\n"
"    -n   --nostdopts            do not add standard LizardFS mount options: '-o " DEFAULT_OPTIONS ",fsname=MFS'\n"
"    -o mfscfgfile=CFGFILE       load some mount options from external file (if not specified then use default file: " ETC_PATH "/mfsmount.cfg)\n"
"    -o mfsdebug                 print some debugging information\n"
"    -o mfsmeta                  mount meta filesystem (trash etc.)\n"
"    -o mfsdelayedinit           connection with master is done in background - with this option mount can be run without network (good for being run from fstab / init scripts etc.)\n"
"    -o mfsacl                   enable ACL support (disabled by default)\n"
"    -o mfsrwlock=0|1            when set to 1, parallel reads from the same descriptor are"
		" performed (default: 1)\n"
#ifdef __linux__
"    -o mfsmkdircopysgid=N       sgid bit should be copied during mkdir operation (default: 1)\n"
#else
"    -o mfsmkdircopysgid=N       sgid bit should be copied during mkdir operation (default: 0)\n"
#endif
#if defined(DEFAULT_SUGID_CLEAR_MODE_EXT)
"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: EXT)\n"
#elif defined(DEFAULT_SUGID_CLEAR_MODE_BSD)
"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: BSD)\n"
#elif defined(DEFAULT_SUGID_CLEAR_MODE_OSX)
"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: OSX)\n"
#else
"    -o mfssugidclearmode=SMODE  set sugid clear mode (see below ; default: NEVER)\n"
#endif
"    -o mfscachemode=CMODE       set cache mode (see below ; default: AUTO)\n"
"    -o mfscachefiles            (deprecated) equivalent to '-o mfscachemode=YES'\n"
"    -o mfsattrcacheto=SEC       set attributes cache timeout in seconds (default: 1.0)\n"
"    -o mfsentrycacheto=SEC      set file entry cache timeout in seconds (default: 0.0)\n"
"    -o mfsdirentrycacheto=SEC   set directory entry cache timeout in seconds (default: 0.25)\n"
"    -o mfsdirentrycachesize=N   define directory entry cache size in number of entries (default: 100000)\n"
"    -o mfsaclcacheto=SEC        set ACL cache timeout in seconds (default: 1.0)\n"
"    -o mfsreportreservedperiod=SEC set reporting reserved inodes interval in seconds (default: 60)\n"
"    -o mfschunkserverrtt=MSEC   set timeout after which SYN packet is considered lost during the first retry of connecting a chunkserver (default: " STR(LIZARDFS_MOUNT_DEFAULT_RTT) ")\n"
"    -o mfschunkserverconnectreadto=MSEC set timeout for connecting with chunkservers during read operation in milliseconds (default: " STR(LIZARDFS_MOUNT_DEFAULT_CHUNKSERVERREADTO) ")\n"
"    -o mfschunkserverwavereadto=MSEC   set timeout for executing each wave of a read operation in milliseconds (default: " STR(LIZARDFS_MOUNT_DEFAULT_CHUNKSERVERWAVEREADTO) ")\n"
"    -o mfschunkservertotalreadto=MSEC   set timeout for the whole communication with chunkservers during a read operation in milliseconds (default: " STR(LIZARDFS_MOUNT_DEFAULT_CHUNKSERVERREADTO) ")\n"
"    -o cacheexpirationtime=MSEC set timeout for read cache entries to be considered valid in milliseconds (0 disables cache) (default: " STR(LIZARDFS_MOUNT_DEFAULT_CACHE_EXPIRATION_TIME_MS) ")\n"
"    -o readaheadmaxwindowsize=KB set max value of readahead window per single descriptor in kibibytes (default: " STR(LIZARDFS_MOUNT_DEFAULT_CACHE_EXPIRATION_TIME_MS) ")\n"
"    -o mfsprefetchxorstripes    prefetch full xor stripe on every first read of a xor chunk\n"
"    -o mfschunkserverwriteto=MSEC       set chunkserver response timeout during write operation in milliseconds (default: " STR(LIZARDFS_MOUNT_DEFAULT_CHUNKSERVERWRITETO) ")\n"
"    -o mfsnice=N                on startup mfsmount tries to change his 'nice' value (default: -19)\n"
#ifdef MFS_USE_MEMLOCK
"    -o mfsmemlock               try to lock memory\n"
#endif
"    -o mfswritecachesize=N      define size of write cache in MiB (default: 128)\n"
"    -o mfsaclcachesize=N        define ACL cache size in number of entries (0: no cache; default: 1000)\n"
"    -o mfscacheperinodepercentage  define what part of the write cache non occupied by other inodes"
"                                   can a single inode occupy (in %%, default: 25)\n"
"    -o mfswriteworkers=N        define number of write workers (default: 10)\n"
"    -o mfsioretries=N           define number of retries before I/O error is returned (default: 30)\n"
"    -o mfswritewindowsize=N     define write window size (in blocks) for each chunk (default: 15)\n"
"    -o mfsmaster=HOST           define mfsmaster location (default: mfsmaster)\n"
"    -o mfsport=PORT             define mfsmaster port number (default: 9421)\n"
"    -o mfsbind=IP               define source ip address for connections (default: NOT DEFINED - chosen automatically by OS)\n"
"    -o mfssubfolder=PATH        define subfolder to mount as root (default: /)\n"
"    -o mfspassword=PASSWORD     authenticate to mfsmaster with password\n"
"    -o mfsmd5pass=MD5           authenticate to mfsmaster using directly given md5 (only if mfspassword is not defined)\n"
"    -o askpassword              show prompt and ask user for password\n"
"    -o mfsdonotrememberpassword do not remember password in memory - more secure, but when session is lost then new session is created without password\n"
"    -o mfsiolimits=FILE         define I/O limits configuration file\n"
"    -o symlinkcachetimeout=N    define timeout of symlink cache in seconds (default: 3600)\n"
"    -o bandwidthoveruse=N       define ratio of allowed bandwidth overuse when fetching data (default: 1.25)\n"
#if FUSE_VERSION >= 26
"    -o enablefilelocks=0|1      enables/disables global file locking (disabled by default)\n"
#endif
"\n");
	fprintf(stderr,
"CMODE can be set to:\n"
"    NO,NONE or NEVER            never allow files data to be kept in cache (safest but can reduce efficiency)\n"
"    YES or ALWAYS               always allow files data to be kept in cache (dangerous)\n"
"    AUTO                        file cache is managed by mfsmaster automatically (should be very safe and efficient)\n"
"\n");
	fprintf(stderr,
"SMODE can be set to:\n"
"    NEVER                       LizardFS will not change suid and sgid bit on chown\n"
"    ALWAYS                      clear suid and sgid on every chown - safest operation\n"
"    OSX                         standard behavior in OS X and Solaris (chown made by unprivileged user clear suid and sgid)\n"
"    BSD                         standard behavior in *BSD systems (like in OSX, but only when something is really changed)\n"
"    EXT                         standard behavior in most file systems on Linux (directories not changed, others: suid cleared always, sgid only when group exec bit is set)\n"
"    XFS                         standard behavior in XFS on Linux (like EXT but directories are changed by unprivileged users)\n"
"SMODE extra info:\n"
"    btrfs,ext2,ext3,ext4,hfs[+],jfs,ntfs and reiserfs on Linux work as 'EXT'.\n"
"    Only xfs on Linux works a little different. Beware that there is a strange\n"
"    operation - chown(-1,-1) which is usually converted by a kernel into something\n"
"    like 'chmod ug-s', and therefore can't be controlled by MFS as 'chown'\n"
"\n");
#undef STR_AUX
#undef STR
}

void mfs_opt_parse_cfg_file(const char *filename,int optional,struct fuse_args *outargs) {
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
	gCustomCfg = 1;
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
					if (gDefaultMountpoint) {
						free(gDefaultMountpoint);
					}
					gDefaultMountpoint = strdup(p);
				} else {
					fuse_opt_add_arg(outargs,"-o");
					fuse_opt_add_arg(outargs,p);
				}
			}
		}
	}
	fclose(fd);
}

// Function for FUSE: has to have these arguments
int mfs_opt_proc_stage1(void *data, const char *arg, int key, struct fuse_args *outargs) {
	struct fuse_args *defargs = (struct fuse_args*)data;
	(void)outargs; // remove unused argument warning

	if (key==KEY_CFGFILE) {
		if (memcmp(arg,"mfscfgfile=",11)==0) {
			mfs_opt_parse_cfg_file(arg+11,0,defargs);
		} else if (arg[0]=='-' && arg[1]=='c') {
			mfs_opt_parse_cfg_file(arg+2,0,defargs);
		}
		return 0;
	}
	return 1;
}

// Function for FUSE: has to have these arguments
// return value:
//   0 - discard this arg
//   1 - keep this arg for future processing
int mfs_opt_proc_stage2(void *data, const char *arg, int key, struct fuse_args *outargs) {
	(void)data; // remove unused argument warning

	switch (key) {
	case FUSE_OPT_KEY_OPT:
		return 1;
	case FUSE_OPT_KEY_NONOPT:
		return 1;
	case KEY_HOST:
		if (gMountOptions.masterhost!=NULL) {
			free(gMountOptions.masterhost);
		}
		gMountOptions.masterhost = strdup(arg+2);
		return 0;
	case KEY_PORT:
		if (gMountOptions.masterport!=NULL) {
			free(gMountOptions.masterport);
		}
		gMountOptions.masterport = strdup(arg+2);
		return 0;
	case KEY_BIND:
		if (gMountOptions.bindhost!=NULL) {
			free(gMountOptions.bindhost);
		}
		gMountOptions.bindhost = strdup(arg+2);
		return 0;
	case KEY_PATH:
		if (gMountOptions.subfolder!=NULL) {
			free(gMountOptions.subfolder);
		}
		gMountOptions.subfolder = strdup(arg+2);
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
	case KEY_VERSION:
		fprintf(stderr, "LizardFS version %s\n", LIZARDFS_PACKAGE_VERSION);
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
