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

#include "common/server_connection.h"
#include "master/quota_database.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"
#include "tools/tools_commands.h"
#include "tools/tools_common_functions.h"

static void quota_set_usage() {
	fprintf(stderr,
	        "set quotas\n\n"
	        "usage:\n lizardfs setquota (-u <uid>|-g <gid> |-d) "
	        "<soft-limit-size> <hard-limit-size> "
	        "<soft-limit-inodes> <hard-limit-inodes> <directory-path>\n"
	        " 0 deletes the limit\n");
}

static int quota_set(const std::string &path, QuotaOwner owner, uint64_t soft_inodes,
					 uint64_t hard_inodes, uint64_t soft_size, uint64_t hard_size) {
	uint32_t inode;
	int fd = open_master_conn(path.c_str(), &inode, nullptr, true);
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
		if (check_usage(quota_set_usage, inode != 1, "Mount root path expected\n")) {
			return 1;
		}
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

int quota_set_run(int argc, char **argv) {
	std::vector<int> uid;
	std::vector<int> gid;
	bool reportAll = false;
	bool per_directory_quota = false;
	char *endptr = nullptr;
	std::string dir_path;
	int ch;

	while ((ch = getopt(argc, argv, "du:g:")) != -1) {
		switch (ch) {
		case 'u':
			uid.push_back(strtol(optarg, &endptr, 10));
			if (check_usage(quota_set_usage, *endptr, "invalid uid: %s\n", optarg)) {
				return 1;
			}
			break;
		case 'g':
			gid.push_back(strtol(optarg, &endptr, 10));
			if (check_usage(quota_set_usage, *endptr, "invalid gid: %s\n", optarg)) {
				return 1;
			}
			break;
		case 'd':
			per_directory_quota = true;
			break;
		default:
			fprintf(stderr, "invalid argument: %c", (char)ch);
			quota_set_usage();
			return 1;
		}
	}
	if (check_usage(quota_set_usage,
	            !((uid.size() + gid.size() != 0) ^ (reportAll || per_directory_quota)),
	            "provide either -a flag or uid/gid\n")) {
		return 1;
	}
	if (check_usage(quota_set_usage, !per_directory_quota && (uid.size() + gid.size() != 1),
	            "provide a single user/group id\n")) {
		return 1;
	}

	argc -= optind;
	argv += optind;

	if (check_usage(quota_set_usage, argc != 5,
	            "expected parameters: <hard-limit-size> <soft-limit-size> "
	            "<hard-limit-inodes> <soft-limit-inodes> <mountpoint-root-path>\n")) {
		return 1;
	}
	uint64_t quotaSoftInodes = 0, quotaHardInodes = 0, quotaSoftSize = 0, quotaHardSize = 0;
	if (check_usage(quota_set_usage, my_get_number(argv[0], &quotaSoftSize, UINT64_MAX, 1) < 0,
	            "soft-limit-size bad value\n")) {
		return 1;
	}
	if (check_usage(quota_set_usage, my_get_number(argv[1], &quotaHardSize, UINT64_MAX, 1) < 0,
	            "hard-limit-size bad value\n")) {
		return 1;
	}
	if (check_usage(quota_set_usage, my_get_number(argv[2], &quotaSoftInodes, UINT64_MAX, 0) < 0,
	            "soft-limit-inodes bad value\n")) {
		return 1;
	}
	if (check_usage(quota_set_usage, my_get_number(argv[3], &quotaHardInodes, UINT64_MAX, 0) < 0,
	            "hard-limit-inodes bad value\n")) {
		return 1;
	}

	QuotaOwner quotaOwner;
	if (!per_directory_quota) {
		sassert((uid.size() + gid.size() == 1));
		quotaOwner = ((uid.size() == 1) ? QuotaOwner(QuotaOwnerType::kUser, uid[0])
		                                : QuotaOwner(QuotaOwnerType::kGroup, gid[0]));
	} else {
		quotaOwner = QuotaOwner(QuotaOwnerType::kInode, 0);
	}

	dir_path = argv[4];

	return quota_set(dir_path, quotaOwner, quotaSoftInodes, quotaHardInodes, quotaSoftSize,
	                 quotaHardSize);
}
