/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA,
   2013-2019 Skytechnology sp. z o.o.

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

#include <fcntl.h>

#include "mount/client_common.h"
#include "mount/special_inode.h"
#include "mount/stats.h"

using namespace LizardClient;

namespace InodeMasterInfo {
static void open(const Context &ctx, FileInfo *fi) {
	if ((fi->flags & O_ACCMODE) != O_RDONLY) {
		oplog_printf(ctx, "open (%lu) (internal node: MASTERINFO): %s",
		            (unsigned long int)inode_, lizardfs_error_string(LIZARDFS_ERROR_EACCES));
		throw RequestException(LIZARDFS_ERROR_EACCES);
	}
	fi->fh = 0;
	fi->direct_io = 0;
	fi->keep_cache = 1;
	oplog_printf(ctx, "open (%lu) (internal node: MASTERINFO): OK (0,1)",
	            (unsigned long int)inode_);
}
} // InodeMasterInfo

namespace InodeStats {
static void open(const Context &ctx, FileInfo *fi) {
	sinfo *statsinfo;
	statsinfo = (sinfo*) malloc(sizeof(sinfo));
	if (!statsinfo) {
		oplog_printf(ctx, "open (%lu) (internal node: STATS): %s",
		            (unsigned long int)inode_,
		            lizardfs_error_string(LIZARDFS_ERROR_OUTOFMEMORY));
		throw RequestException(LIZARDFS_ERROR_OUTOFMEMORY);
	}
	if (pthread_mutex_init(&(statsinfo->lock), NULL))  {
		free(statsinfo);
		throw RequestException(LIZARDFS_ERROR_EPERM);
	}
	PthreadMutexWrapper lock((statsinfo->lock));         // make helgrind happy
	stats_show_all(&(statsinfo->buff),&(statsinfo->leng));
	statsinfo->reset = 0;
	fi->fh = reinterpret_cast<uintptr_t>(statsinfo);
	fi->direct_io = 1;
	fi->keep_cache = 0;
	oplog_printf(ctx, "open (%lu) (internal node: STATS): OK (1,0)",
	            (unsigned long int)inode_);
}
} // InodeStats

namespace InodeOplog {
static void open(const Context &ctx, FileInfo *fi) {
	if ((fi->flags & O_ACCMODE) != O_RDONLY) {
		oplog_printf(ctx, "open (%lu) (internal node: OPLOG): %s",
		            (unsigned long int)inode_,
		            lizardfs_error_string(LIZARDFS_ERROR_EACCES));
		throw RequestException(LIZARDFS_ERROR_EACCES);
	}
	fi->fh = oplog_newhandle(0);
	fi->direct_io = 1;
	fi->keep_cache = 0;
	oplog_printf(ctx, "open (%lu) (internal node: OPLOG): OK (1,0)",
	            (unsigned long int)inode_);
}
} // InodeOplog

namespace InodeOphistory {
static void open(const Context &ctx, FileInfo *fi) {
	if ((fi->flags & O_ACCMODE) != O_RDONLY) {
		oplog_printf(ctx, "open (%lu) (internal node: OPHISTORY): %s",
		            (unsigned long int)inode_,
		            lizardfs_error_string(LIZARDFS_ERROR_EACCES));
		throw RequestException(LIZARDFS_ERROR_EACCES);
	}
	fi->fh = oplog_newhandle(1);
	fi->direct_io = 1;
	fi->keep_cache = 0;
	oplog_printf(ctx, "open (%lu) (internal node: OPHISTORY): OK (1,0)",
	            (unsigned long int)inode_);
}
} // InodeOphistory

namespace InodeTweaks {
static void open(const Context &ctx, FileInfo *fi) {
	MagicFile *file = new MagicFile;
	fi->fh = reinterpret_cast<uintptr_t>(file);
	fi->direct_io = 1;
	fi->keep_cache = 0;
	oplog_printf(ctx, "open (%lu) (internal node: TWEAKS_FILE): OK (1,0)",
	            (unsigned long int)inode_);
}
} // InodeTweaks

static const std::array<std::function<void
	(const Context&, FileInfo*)>, 16> funcs = {{
	 &InodeStats::open,             //0x0U
	 &InodeOplog::open,             //0x1U
	 &InodeOphistory::open,         //0x2U
	 &InodeTweaks::open,            //0x3U
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
	 &InodeMasterInfo::open         //0xFU
}};

void special_open(Inode ino, const Context &ctx, FileInfo *fi) {
	auto func = funcs[ino - SPECIAL_INODE_BASE];
	if (!func) {
		lzfs_pretty_syslog(LOG_WARNING,
			"Trying to call unimplemented 'open' function for special inode");
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}
	return func(ctx, fi);
}
