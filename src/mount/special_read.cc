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

static void printDebugReadInfo(Inode ino, uint64_t size, uint64_t off) {
	fprintf(stderr,"read from inode %u up to %" PRIu64 " bytes from position %" PRIu64 "\n",
	        (unsigned int)ino, size, off);
}

static void printReadOplogOk(const Context &ctx, Inode ino, uint64_t size, uint64_t off,
	                            unsigned long int size_read) {
	oplog_printf(ctx, "read (%u,%" PRIu64 ",%" PRIu64 "): OK (%lu)",
	            (unsigned int)ino, size, off, size_read);
}

static void printReadOplogNoData(const Context &ctx, Inode ino, uint64_t size, uint64_t off) {
	oplog_printf(ctx, "read (%u,%" PRIu64 ",%" PRIu64 "): OK (no data)",
	            (unsigned int)ino, size, off);
}

namespace InodeMasterInfo {
static std::vector<uint8_t> read(const Context &ctx, size_t size, off_t off,
	                          FileInfo */*fi*/, int debug_mode) {
	if (debug_mode) {
		printDebugReadInfo(SPECIAL_INODE_MASTERINFO, size, off);
	}
	std::vector<uint8_t> ret;
	uint8_t masterinfo[14];
	fs_getmasterlocation(masterinfo);
	masterproxy_getlocation(masterinfo);

	if (off >= 14) {
		printReadOplogNoData(ctx,
		                    SPECIAL_INODE_MASTERINFO,
		                    (uint64_t)size,
		                    (uint64_t)off);
	} else if (off + size > 14) {
		std::copy(masterinfo + off, masterinfo + 14, std::back_inserter(ret));
		printReadOplogOk(ctx,
		                SPECIAL_INODE_MASTERINFO,
		                (uint64_t)size,
		                (uint64_t)off,
		                (unsigned long int)(14 - off));
	} else {
		std::copy(masterinfo + off, masterinfo + off + size, std::back_inserter(ret));
		printReadOplogOk(ctx,
		                SPECIAL_INODE_MASTERINFO,
		                (uint64_t)size,
		                (uint64_t)off,
		                (unsigned long int)size);
	}
	return ret;
}
} // InodeMasterInfo

namespace InodeStats {
static std::vector<uint8_t> read(const Context &ctx,
		size_t size, off_t off, FileInfo *fi, int debug_mode) {
	if (debug_mode) {
		printDebugReadInfo(SPECIAL_INODE_STATS, size, off);
	}
	std::vector<uint8_t> ret;
	sinfo *statsinfo = reinterpret_cast<sinfo*>(fi->fh);
	if (statsinfo != NULL) {
		PthreadMutexWrapper lock((statsinfo->lock));         // make helgrind happy

		if (off >= statsinfo->leng) {
			printReadOplogNoData(ctx,
			                    SPECIAL_INODE_STATS,
			                    (uint64_t)size,
			                    (uint64_t)off);
		} else if ((uint64_t)(off + size) > (uint64_t)(statsinfo->leng)) {
			std::copy(statsinfo->buff + off, statsinfo->buff + statsinfo->leng,
			          std::back_inserter(ret));
			printReadOplogOk(ctx,
			                SPECIAL_INODE_STATS,
			                (uint64_t)size,
			                (uint64_t)off,
			                (unsigned long int)(statsinfo->leng-off));
		} else {
			std::copy(statsinfo->buff + off, statsinfo->buff + off + size,
			          std::back_inserter(ret));
			printReadOplogOk(ctx,
			                SPECIAL_INODE_STATS,
			                (uint64_t)size,
			                (uint64_t)off,
			                (unsigned long int)size);
		}
	} else {
		printReadOplogNoData(ctx,
		                    SPECIAL_INODE_STATS,
		                    (uint64_t)size,
		                    (uint64_t)off);
	}
	return ret;
}
} // InodeStats

namespace InodeOplog {
static std::vector<uint8_t> read(const Context &ctx,
		size_t size, off_t off, FileInfo *fi, int debug_mode) {
	if (debug_mode) {
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 ") ...",
		            (unsigned long int)SPECIAL_INODE_OPLOG,
		            (uint64_t)size,
		            (uint64_t)off);

		printDebugReadInfo(SPECIAL_INODE_OPLOG, size, off);
	}
	uint32_t ssize;
	uint8_t *buff;
	oplog_getdata(fi->fh, &buff, &ssize, size);
	oplog_releasedata(fi->fh);
	return std::vector<uint8_t>(buff, buff + ssize);
}
} // InodeOplog

namespace InodeOphistory {
static std::vector<uint8_t> read(const Context &ctx,
		size_t size, off_t off, FileInfo *fi, int debug_mode) {
	if (debug_mode) {
		oplog_printf(ctx, "read (%lu,%" PRIu64 ",%" PRIu64 ") ...",
		            (unsigned long int)SPECIAL_INODE_OPHISTORY,
		            (uint64_t)size,
		            (uint64_t)off);

		printDebugReadInfo(SPECIAL_INODE_OPHISTORY, size, off);
	}
	uint32_t ssize;
	uint8_t *buff;
	oplog_getdata(fi->fh, &buff, &ssize, size);
	oplog_releasedata(fi->fh);
	return std::vector<uint8_t>(buff, buff + ssize);
}
} // InodeOphistory

namespace InodeTweaks {
static std::vector<uint8_t> read(const Context &ctx,
		size_t size, off_t off, FileInfo *fi, int debug_mode) {
	if (debug_mode) {
		printDebugReadInfo(SPECIAL_INODE_TWEAKS, size, off);
	}
	MagicFile *file = reinterpret_cast<MagicFile*>(fi->fh);
	std::unique_lock<std::mutex> lock(file->mutex);
	if (!file->wasRead) {
		file->value = gTweaks.getAllValues();
		file->wasRead = true;
	}
	if (off >= static_cast<off_t>(file->value.size())) {
		printReadOplogNoData(ctx,
		                    SPECIAL_INODE_TWEAKS,
		                    (uint64_t)size,
		                    (uint64_t)off);
		return std::vector<uint8_t>();
	} else {
		size_t availableBytes = size;
		if ((uint64_t)(off + size) > (uint64_t)file->value.size()) {
			availableBytes = file->value.size() - off;
		}
		const uint8_t *data = reinterpret_cast<const uint8_t*>(file->value.data()) + off;
		printReadOplogOk(ctx,
		                SPECIAL_INODE_TWEAKS,
		                (uint64_t)size,
		                (uint64_t)off,
		                (unsigned long int)availableBytes);
		return std::vector<uint8_t>(data, data + availableBytes);
	}
}
} // InodeTweaks

static const std::array<std::function<std::vector<uint8_t>
	(const Context&, size_t, off_t, FileInfo*, int)>, 16> funcs = {{
	 &InodeStats::read,             //0x0U
	 &InodeOplog::read,             //0x1U
	 &InodeOphistory::read,         //0x2U
	 &InodeTweaks::read,            //0x3U
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
	 &InodeMasterInfo::read         //0xFU
}};

std::vector<uint8_t> special_read(Inode ino, const Context &ctx, size_t size, off_t off,
	                          FileInfo *fi, int debug_mode) {
	auto func = funcs[ino - SPECIAL_INODE_BASE];
	if (!func) {
		lzfs_pretty_syslog(LOG_WARNING,
			"Trying to call unimplemented 'read' function for special inode");
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}
	return func(ctx, size, off, fi, debug_mode);
}
