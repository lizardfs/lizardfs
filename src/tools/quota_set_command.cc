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

#include "common/platform.h"

#include <stdio.h>
#include <stdlib.h>

#include "tools/tools_commands.h"

int quota_set(const std::string &path, QuotaOwner owner, uint64_t soft_inodes, uint64_t hard_inodes,
			  uint64_t soft_size, uint64_t hard_size) {
	uint32_t inode;
	int fd = open_master_conn(path.c_str(), &inode, nullptr, 0, 1);
	if (fd < 0) {
		return -1;
	}

	uint32_t uid = getuid();
	uint32_t gid = getgid();

	if (owner.ownerType == QuotaOwnerType::kInode) {
		owner.ownerId = inode;
	}

	std::vector<QuotaEntry> quota_entries{
	    {QuotaEntryKey(owner, QuotaRigor::kSoft, QuotaResource::kInodes), soft_inodes},
	    {QuotaEntryKey(owner, QuotaRigor::kHard, QuotaResource::kInodes), hard_inodes},
	    {QuotaEntryKey(owner, QuotaRigor::kSoft, QuotaResource::kSize), soft_size},
	    {QuotaEntryKey(owner, QuotaRigor::kHard, QuotaResource::kSize), hard_size},
	};

	uint32_t message_id = 0;
	auto request = cltoma::fuseSetQuota::build(message_id, uid, gid, quota_entries);
	if (owner.ownerType != QuotaOwnerType::kInode) {
		check_usage(MFSSETQUOTA, inode != 1, "Mount root path expected\n");
	}
	try {
		auto response = ServerConnection::sendAndReceive(fd, request, LIZ_MATOCL_FUSE_SET_QUOTA);
		uint8_t status;
		matocl::fuseSetQuota::deserialize(response, message_id, status);
		if (status != LIZARDFS_STATUS_OK) {
			throw Exception(std::string(path) + ": failed", status);
		}
	} catch (Exception &e) {
		fprintf(stderr, "%s\n", e.what());
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	return 0;
}
