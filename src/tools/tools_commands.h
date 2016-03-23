/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2016 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

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

#include <string>
#include <stdint.h>
#include <tools/tools_common_functions.h>
#include "master/quota_database.h"
#include "protocol/matocl.h"
#include "protocol/cltoma.h"
#include "protocol/MFSCommunication.h"
#include "common/server_connection.h"
#include "common/datapack.h"
#include "common/mfserr.h"

int append_file(const char *fname, const char *afname);
int check_file(const char *fname);
int dir_info(const char *fname);
int file_info(const char *fileName);
int file_repair(const char *fname);
int snapshot(const char *dstname, char *const *srcnames, uint32_t srcelements, uint8_t canowerwrite,
			 int long_wait);

int get_eattr(const char *fname, uint8_t mode);
int set_eattr(const char *fname, uint8_t eattr, uint8_t mode);

int get_goal(const char *fname, uint8_t mode);
int set_goal(const char *fname, const std::string &goal, uint8_t mode);

int get_trashtime(const char *fname, uint8_t mode);
int set_trashtime(const char *fname, uint32_t trashtime, uint8_t mode);

int quota_rep(const std::string &path, std::vector<int> requested_uids,
			  std::vector<int> requested_gid, bool report_all, bool per_directory_quota);
int quota_set(const std::string &path, QuotaOwner owner, uint64_t soft_inodes, uint64_t hard_inodes,
			  uint64_t soft_size, uint64_t hard_size);
