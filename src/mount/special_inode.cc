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

#include <fcntl.h>

#include "mount/special_inode.h"
#include "mount/stats.h"

using namespace LizardClient;

#ifdef MASTERINFO_WITH_VERSION
const Attributes InodeMasterInfo::attr =
	  {{'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,14}};
#else
const Attributes InodeMasterInfo::attr =
	  {{'f', 0x01,0x24, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,10}};
#endif
const Inode InodeMasterInfo::inode_ = SPECIAL_INODE_MASTERINFO;

// 0x01A4 == 0b110100100 == 0644
const Attributes InodeStats::attr =
	  {{'f', 0x01,0xA4, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0}};
const Inode InodeStats::inode_ = SPECIAL_INODE_STATS;

// 0x0100 == 0b100000000 == 0400
const Attributes InodeOplog::attr =
	  {{'f', 0x01,0x00, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0}};
const Inode InodeOplog::inode_ = SPECIAL_INODE_OPLOG;

// 0x0100 == 0b100000000 == 0400
const Attributes InodeOphistory::attr =
	  {{'f', 0x01,0x00, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0}};
const Inode InodeOphistory::inode_ = SPECIAL_INODE_OPHISTORY;

// 0x01A4 == 0b110100100 == 0644
const Attributes InodeTweaks::attr =
	  {{'f', 0x01,0xA4, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0}};
const Inode InodeTweaks::inode_ = SPECIAL_INODE_TWEAKS;

// 0x01ED == 0b111101101 == 0755
const Attributes InodeFileByInode::attr =
	  {{'d', 0x01,0xED, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,1, 0,0,0,0,0,0,0,0}};
const Inode InodeFileByInode::inode_ = SPECIAL_INODE_FILE_BY_INODE;
