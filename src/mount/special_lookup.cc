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

using namespace LizardClient;

namespace InodeMasterInfo {
static EntryParam lookup(const Context &ctx, Inode parent, const char *name,
	                           char attrstr[256]) {
	EntryParam e;
	e.ino = inode_;
	e.attr_timeout = 3600.0;
	e.entry_timeout = 3600.0;
	attr_to_stat(inode_, attr, &e.attr);
	stats_inc(OP_LOOKUP_INTERNAL);
	makeattrstr(attrstr, 256, &e.attr);
	oplog_printf(ctx, "lookup (%lu,%s) (internal node: MASTERINFO): OK (%.1f,%lu,%.1f,%s)",
	            (unsigned long int)parent,
	            name,
	            e.entry_timeout,
	            (unsigned long int)e.ino,
	            e.attr_timeout,
	            attrstr);
	return e;
}
} // InodeMasterInfo

namespace InodeStats {
static EntryParam lookup(const Context &ctx, Inode parent, const char *name,
	                      char attrstr[256]) {
	EntryParam e;
	e.ino = inode_;
	e.attr_timeout = 3600.0;
	e.entry_timeout = 3600.0;
	attr_to_stat(inode_, attr, &e.attr);
	stats_inc(OP_LOOKUP_INTERNAL);
	makeattrstr(attrstr, 256, &e.attr);
	oplog_printf(ctx, "lookup (%lu,%s) (internal node: STATS): OK (%.1f,%lu,%.1f,%s)",
	            (unsigned long int)parent,
	            name,
	            e.entry_timeout,
	            (unsigned long int)e.ino,
	            e.attr_timeout,
	            attrstr);
	return e;
}
}  //InodeStats

namespace InodeOplog {
static EntryParam lookup(const Context &ctx, Inode parent, const char *name,
	                      char attrstr[256]) {
	EntryParam e;
	e.ino = inode_;
	e.attr_timeout = 3600.0;
	e.entry_timeout = 3600.0;
	attr_to_stat(inode_, attr, &e.attr);
	stats_inc(OP_LOOKUP_INTERNAL);
	makeattrstr(attrstr, 256, &e.attr);
	oplog_printf(ctx, "lookup (%lu,%s) (internal node: OPLOG): OK (%.1f,%lu,%.1f,%s)",
	            (unsigned long int)parent,
	            name,
	            e.entry_timeout,
	            (unsigned long int)e.ino,
	            e.attr_timeout,
	            attrstr);
	return e;
}
} // InodeOplog

namespace InodeOphistory {
static EntryParam lookup(const Context &ctx, Inode parent, const char *name,
	                          char attrstr[256]) {
	EntryParam e;
	e.ino = inode_;
	e.attr_timeout = 3600.0;
	e.entry_timeout = 3600.0;
	attr_to_stat(inode_, attr, &e.attr);
	stats_inc(OP_LOOKUP_INTERNAL);
	makeattrstr(attrstr, 256, &e.attr);
	oplog_printf(ctx, "lookup (%lu,%s) (internal node: OPHISTORY): OK (%.1f,%lu,%.1f,%s)",
	            (unsigned long int)parent,
	            name,
	            e.entry_timeout,
	            (unsigned long int)e.ino,
	            e.attr_timeout,
	            attrstr);
	return e;
}
} // InodeOphistory

namespace InodeTweaks {
static EntryParam lookup(const Context &ctx, Inode parent, const char *name,
	                       char attrstr[256]) {
	EntryParam e;
	e.ino = inode_;
	e.attr_timeout = 3600.0;
	e.entry_timeout = 3600.0;
	attr_to_stat(inode_, attr, &e.attr);
	stats_inc(OP_LOOKUP_INTERNAL);
	makeattrstr(attrstr, 256, &e.attr);
	oplog_printf(ctx, "lookup (%lu,%s) (internal node: TWEAKS_FILE): OK (%.1f,%lu,%.1f,%s)",
	            (unsigned long int)parent,
	            name,
	            e.entry_timeout,
	            (unsigned long int)e.ino,
	            e.attr_timeout,
	            attrstr);
	return e;
}
} // InodeTweaks

namespace InodeFileByInode {
static EntryParam lookup(const Context &ctx, Inode parent, const char *name,
	                            char attrstr[256]) {
	EntryParam e;
	e.ino = inode_;
	e.attr_timeout = 3600.0;
	e.entry_timeout = 3600.0;
	attr_to_stat(inode_, attr, &e.attr);
	stats_inc(OP_LOOKUP_INTERNAL);
	makeattrstr(attrstr, 256, &e.attr);
	oplog_printf(ctx, "lookup (%lu,%s) (internal node: FILE_BY_INODE_FILE): OK (%.1f,%lu,%.1f,%s)",
	            (unsigned long int)parent,
	            name,
	            e.entry_timeout,
	            (unsigned long int)e.ino,
	            e.attr_timeout,
	            attrstr);
	return e;
}
} // InodeFileByInode

static const std::array<std::function<EntryParam
	(const Context&, Inode, const char*, char[256])>, 16> funcs = {{
	 &InodeStats::lookup,           //0x0U
	 &InodeOplog::lookup,           //0x1U
	 &InodeOphistory::lookup,       //0x2U
	 &InodeTweaks::lookup,          //0x3U
	 &InodeFileByInode::lookup,     //0x4U
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
	 &InodeMasterInfo::lookup       //0xFU
}};

EntryParam special_lookup(Inode ino, const Context &ctx, Inode parent, const char *name,
	                  char attrstr[256]) {
	auto func = funcs[ino - SPECIAL_INODE_BASE];
	if (!func) {
		lzfs_pretty_syslog(LOG_WARNING,
			"Trying to call unimplemented 'lookup' function for special inode");
		throw RequestException(LIZARDFS_ERROR_EINVAL);
	}
	return func(ctx, parent, name, attrstr);
}
