/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2015 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

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
#include "common/goal_map.h"

#define DEFAULT_GOAL 1
#define DEFAULT_TRASHTIME 86400

const GoalMap<Goal> &fs_get_goal_definitions();
const Goal &fs_get_goal_definition(uint8_t goalId);

void fs_broadcast_metadata_saved(uint8_t status);

// Adds an entry to a changelog, updates filesystem.cc internal structures, prepends a
// proper timestamp to changelog entry and broadcasts it to metaloggers and shadow masters
void fs_changelog(uint32_t ts, const char *format, ...) __attribute__((__format__(__printf__, 2, 3)));
void fs_add_files_to_chunks();

uint32_t fs_do_emptyreserved(uint32_t ts);
uint64_t fs_getversion();
uint8_t fs_repair(uint32_t rootinode, uint8_t sesflags, uint32_t inode, uint32_t uid, uint32_t gid,
			uint32_t *notchanged, uint32_t *erased, uint32_t *repaired);
