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

#pragma once

#include "common/platform.h"

#include <stddef.h>
#include <string.h>
#include <fuse.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include "protocol/MFSCommunication.h"
#include "mount/lizard_client.h"

#if defined(LIZARDFS_HAVE_MLOCKALL) && defined(RLIMIT_MEMLOCK)
#  include <sys/mman.h>
#endif

#if defined(MCL_CURRENT) && defined(MCL_FUTURE)
#  define MFS_USE_MEMLOCK
#endif

#if defined(__APPLE__)
#  define DEFAULT_OPTIONS "allow_other,default_permissions,daemon_timeout=600,iosize=65536"
#else
#  define DEFAULT_OPTIONS "allow_other,default_permissions"
#endif

enum {
	KEY_CFGFILE,
	KEY_META,
	KEY_HOST,
	KEY_PORT,
	KEY_BIND,
	KEY_PATH,
	KEY_PASSWORDASK,
	KEY_NOSTDMOUNTOPTIONS,
#if FUSE_VERSION >= 30
	KEY_NONEMPTY,
#endif
	KEY_HELP,
	KEY_VERSION
};

struct mfsopts_ {
	char *masterhost;
	char *masterport;
	char *bindhost;
	char *subfolder;
	char *password;
	char *md5pass;
	unsigned nofile;
	signed nice;
#ifdef MFS_USE_MEMLOCK
	int memlock;
#endif
#if FUSE_VERSION >= 26
	int filelocks;
#endif
	int nostdmountoptions;
	int meta;
	int debug;
	int delayedinit;
	int acl;
	double aclcacheto;
	unsigned aclcachesize;
	int rwlock;
	int mkdircopysgid;
	char *sugidclearmodestr;
	SugidClearMode sugidclearmode;
	char *cachemode;
	int cachefiles;
	int keepcache;
	int passwordask;
	int donotrememberpassword;
	unsigned writecachesize;
	unsigned cachePerInodePercentage;
	unsigned writeworkers;
	unsigned ioretries;
	unsigned writewindowsize;
	double attrcacheto;
	double entrycacheto;
	double direntrycacheto;
	unsigned direntrycachesize;
	unsigned reportreservedperiod;
	char *iolimits;
	int chunkserverrtt;
	int chunkserverconnectreadto;
	int chunkserverwavereadto;
	int chunkservertotalreadto;
	int chunkserverwriteto;
	int cacheexpirationtime;
	int readaheadmaxwindowsize;
	int prefetchxorstripes;
	unsigned symlinkcachetimeout;
	double bandwidthoveruse;
#if FUSE_VERSION >= 30
	int nonemptymount;
#endif

	mfsopts_()
		: masterhost(NULL),
		masterport(NULL),
		bindhost(NULL),
		subfolder(NULL),
		password(NULL),
		md5pass(NULL),
		nofile(0),
		nice(-19),
#ifdef MFS_USE_MEMLOCK
		memlock(0),
#endif
#if FUSE_VERSION >= 26
		filelocks(0),
#endif
		nostdmountoptions(0),
		meta(0),
		debug(LizardClient::FsInitParams::kDefaultDebugMode),
		delayedinit(LizardClient::FsInitParams::kDefaultDelayedInit),
		acl(), // deprecated
		aclcacheto(LizardClient::FsInitParams::kDefaultAclCacheTimeout),
		aclcachesize(LizardClient::FsInitParams::kDefaultAclCacheSize),
		rwlock(LizardClient::FsInitParams::kDefaultUseRwLock),
		mkdircopysgid(LizardClient::FsInitParams::kDefaultMkdirCopySgid),
		sugidclearmodestr(NULL),
		sugidclearmode(LizardClient::FsInitParams::kDefaultSugidClearMode),
		cachemode(NULL),
		cachefiles(0),
		keepcache(LizardClient::FsInitParams::kDefaultKeepCache),
		passwordask(0),
		donotrememberpassword(LizardClient::FsInitParams::kDefaultDoNotRememberPassword),
		writecachesize(LizardClient::FsInitParams::kDefaultWriteCacheSize),
		cachePerInodePercentage(LizardClient::FsInitParams::kDefaultCachePerInodePercentage),
		writeworkers(LizardClient::FsInitParams::kDefaultWriteWorkers),
		ioretries(LizardClient::FsInitParams::kDefaultIoRetries),
		writewindowsize(LizardClient::FsInitParams::kDefaultWriteWindowSize),
		attrcacheto(LizardClient::FsInitParams::kDefaultAttrCacheTimeout),
		entrycacheto(LizardClient::FsInitParams::kDefaultEntryCacheTimeout),
		direntrycacheto(LizardClient::FsInitParams::kDefaultDirentryCacheTimeout),
		direntrycachesize(LizardClient::FsInitParams::kDefaultDirentryCacheSize),
		reportreservedperiod(LizardClient::FsInitParams::kDefaultReportReservedPeriod),
		iolimits(NULL),
		chunkserverrtt(LizardClient::FsInitParams::kDefaultRoundTime),
		chunkserverconnectreadto(LizardClient::FsInitParams::kDefaultChunkserverConnectTo),
		chunkserverwavereadto(LizardClient::FsInitParams::kDefaultChunkserverWaveReadTo),
		chunkservertotalreadto(LizardClient::FsInitParams::kDefaultChunkserverTotalReadTo),
		chunkserverwriteto(LizardClient::FsInitParams::kDefaultChunkserverWriteTo),
		cacheexpirationtime(LizardClient::FsInitParams::kDefaultCacheExpirationTime),
		readaheadmaxwindowsize(LizardClient::FsInitParams::kDefaultReadaheadMaxWindowSize),
		prefetchxorstripes(LizardClient::FsInitParams::kDefaultPrefetchXorStripes),
		symlinkcachetimeout(LizardClient::FsInitParams::kDefaultSymlinkCacheTimeout),
		bandwidthoveruse(LizardClient::FsInitParams::kDefaultBandwidthOveruse)
#if FUSE_VERSION >= 30
		, nonemptymount(LizardClient::FsInitParams::kDefaultNonEmptyMounts)
#endif
	{ }
};

extern mfsopts_ gMountOptions;
extern int gCustomCfg;
extern char *gDefaultMountpoint;
extern fuse_opt gMfsOptsStage1[];
extern fuse_opt gMfsOptsStage2[];

void usage(const char *progname);
void mfs_opt_parse_cfg_file(const char *filename,int optional,struct fuse_args *outargs);
int mfs_opt_proc_stage1(void *data, const char *arg, int key, struct fuse_args *outargs);
int mfs_opt_proc_stage2(void *data, const char *arg, int key, struct fuse_args *outargs);
