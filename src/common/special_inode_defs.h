/*
   Copyright 2015 Skytechnology sp. z o.o.

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

#define SPECIAL_INODE_BASE   0xFFFFFFF0U

#define SPECIAL_INODE_ROOT            0x01U
#define SPECIAL_INODE_MASTERINFO      (SPECIAL_INODE_BASE + 0xFU)
#define SPECIAL_INODE_STATS           (SPECIAL_INODE_BASE + 0x0U)
#define SPECIAL_INODE_OPLOG           (SPECIAL_INODE_BASE + 0x1U)
#define SPECIAL_INODE_OPHISTORY       (SPECIAL_INODE_BASE + 0x2U)
#define SPECIAL_INODE_TWEAKS          (SPECIAL_INODE_BASE + 0x3U)
#define SPECIAL_INODE_FILE_BY_INODE   (SPECIAL_INODE_BASE + 0x4U)
#define SPECIAL_INODE_META_TRASH      (SPECIAL_INODE_BASE + 0x5U)
#define SPECIAL_INODE_META_UNDEL      (SPECIAL_INODE_BASE + 0x6U)
#define SPECIAL_INODE_META_RESERVED   (SPECIAL_INODE_BASE + 0x7U)

#define SPECIAL_FILE_NAME_MASTERINFO      ".masterinfo"
#define SPECIAL_FILE_NAME_STATS           ".stats"
#define SPECIAL_FILE_NAME_OPLOG           ".oplog"
#define SPECIAL_FILE_NAME_OPHISTORY       ".ophistory"
#define SPECIAL_FILE_NAME_TWEAKS          ".lizardfs_tweaks"
#define SPECIAL_FILE_NAME_FILE_BY_INODE   ".lizardfs_file_by_inode"
#define SPECIAL_FILE_NAME_META_TRASH      "trash"
#define SPECIAL_FILE_NAME_META_UNDEL      "undel"
#define SPECIAL_FILE_NAME_META_RESERVED   "reserved"

#define MAX_REGULAR_INODE (SPECIAL_INODE_BASE - 0x01U)
