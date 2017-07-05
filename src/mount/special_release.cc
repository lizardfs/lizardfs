/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2016 Skytechnology sp. z o.o.

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

#include "mount/client_common.h"
#include "mount/special_inode.h"
#include "mount/stats.h"

using namespace LizardClient;

namespace InodeMasterInfo {
static void release(FileInfo */*fi*/) {
	oplog_printf("release (%lu) (internal node: MASTERINFO): OK",
	            (unsigned long int)inode_);
}
} // InodeMasterInfo

namespace InodeStats {
static void release(FileInfo *fi) {
	sinfo *statsinfo = reinterpret_cast<sinfo*>(fi->fh);
	if (statsinfo!=NULL) {
		PthreadMutexWrapper lock((statsinfo->lock));         // make helgrind happy
		if (statsinfo->buff!=NULL) {
			free(statsinfo->buff);
		}
		if (statsinfo->reset) {
			stats_reset_all();
		}
		lock.unlock(); // This unlock is needed, since we want to destroy the mutex
		pthread_mutex_destroy(&(statsinfo->lock));      // make helgrind happy
		free(statsinfo);
	}
	oplog_printf("release (%lu) (internal node: STATS): OK",
	            (unsigned long int)inode_);
}
} // InodeStats

namespace InodeOplog {
static void release(FileInfo *fi) {
	oplog_releasehandle(fi->fh);
	oplog_printf("release (%lu) (internal node: OPLOG): OK",
	            (unsigned long int)inode_);
}
} // InodeOplog

namespace InodeOphistory {
static void release(FileInfo *fi) {
	oplog_releasehandle(fi->fh);
	oplog_printf("release (%lu) (internal node: OPHISTORY): OK",
	            (unsigned long int)inode_);
}
} // InodeOphistory

namespace InodeTweaks {
static void release(FileInfo *fi) {
	MagicFile *file = reinterpret_cast<MagicFile*>(fi->fh);
	if (file->wasWritten) {
		auto separatorPos = file->value.find('=');
		if (separatorPos == file->value.npos) {
			lzfs_pretty_syslog(LOG_INFO, "TWEAKS_FILE: Wrong value '%s'",
			                   file->value.c_str());
		} else {
			std::string name = file->value.substr(0, separatorPos);
			std::string value = file->value.substr(separatorPos + 1);
			if (!value.empty() && value.back() == '\n') {
				value.resize(value.size() - 1);
			}
			gTweaks.setValue(name, value);
			lzfs_pretty_syslog(LOG_INFO, "TWEAKS_FILE: Setting '%s' to '%s'",
			                   name.c_str(), value.c_str());
		}
	}
	delete file;
	oplog_printf("release (%lu) (internal node: TWEAKS_FILE): OK",
	            (unsigned long int)inode_);
}
} // InodeTweaks

typedef void (*ReleaseFunc)(FileInfo *);
static const std::array<ReleaseFunc, 16> funcs = {{
	 &InodeStats::release,          //0x0U
	 &InodeOplog::release,          //0x1U
	 &InodeOphistory::release,      //0x2U
	 &InodeTweaks::release,         //0x3U
	 nullptr,                       //0x5U
	 nullptr,                       //0x6U
	 nullptr,                       //0x7U
	 nullptr,                       //0x8U
	 nullptr,                       //0x9U
	 nullptr,                       //0xAU
	 nullptr,                       //0xBU
	 nullptr,                       //0xCU
	 nullptr,                       //0xDU
	 nullptr,                       //0xEU
	 nullptr,                       //0xEU
	 &InodeMasterInfo::release      //0xFU
}};

void special_release(Inode ino, FileInfo *fi) {
	auto func = funcs[ino - SPECIAL_INODE_BASE];
	if (!func) {
		lzfs_pretty_syslog(LOG_WARNING,
			"Trying to call unimplemented 'release' function for special inode");
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}
	return func(fi);
}
