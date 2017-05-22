/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2017 Skytechnology sp. z o.o..

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

#include "common/defective_file_info.h"

std::vector<DefectiveFileInfo> fs_get_defective_nodes_info(uint8_t requested_flags, uint64_t max_entries,
	                                                   uint64_t &entry_index);


void fs_read_periodic_config_file();
void fs_periodic_master_init();

void fs_test_getdata(uint32_t &loopstart, uint32_t &loopend, uint32_t &files, uint32_t &ugfiles,
			uint32_t &mfiles, uint32_t &chunks, uint32_t &ugchunks, uint32_t &mchunks,
			std::string &report);
void fsnodes_periodic_remove(uint32_t inode);
