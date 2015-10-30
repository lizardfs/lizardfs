/*
   2013-2015 Skytechnology sp. z o.o..

   This file is part of LizardFS.

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

#pragma once

#include "common/platform.h"

#include "master/filesystem_freenode.h"
#include "master/quota_database.h"

bool fsnodes_inode_quota_exceeded(uint32_t uid, uint32_t gid);
bool fsnodes_size_quota_exceeded(uint32_t uid, uint32_t gid);
void fsnodes_quota_register_inode(fsnode *node);
void fsnodes_quota_unregister_inode(fsnode *node);
void fsnodes_quota_update_size(fsnode *node, int64_t delta);
