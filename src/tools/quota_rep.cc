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

#include <limits.h>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "common/chunk_with_address_and_label.h"
#include "common/server_connection.h"
#include "master/quota_database.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"
#include "tools/tools_commands.h"
#include "tools/tools_common_functions.h"

static void quota_rep_usage() {
	fprintf(stderr,
	        "summarize quotas for a user/group or all users and groups\n\n"
	        "usage: \n lizardfs repquota [-nhH] (-u <uid>|-g <gid>)+ <mountpoint-root-path>\n"
	        " lizardfs repquota [-nhH] -a <mountpoint-root-path>\n"
	        " lizardfs repquota [-nhH] -d <directory-path>\n");
	print_numberformat_options();
}

static void quota_putc_plus_or_minus(uint64_t usage, uint64_t soft_limit, uint64_t hard_limit) {
	if ((soft_limit > 0) && (usage > soft_limit)) {
		fputs("+", stdout);
	} else if ((hard_limit > 0) && (usage >= hard_limit)) {
		fputs("+", stdout);
	} else {
		fputs("-", stdout);
	}
}

/*! \brief Print one quota entry.
 * \param path Root inode local path.
 * \param path_inode Path inode.
 * \param owner_type Quota entry owner type. If equals to -1 then do not print entry.
 * \param owner_id Quota entry owner id.
 * \param info Quota description.
 * \param limit Table (3x2) with quota limits. Each entry corresponds to specific
 *              quota type. (The function can only print known quota types that fit in table)
 */

static void quota_print_entry(const std::string &path, uint32_t path_inode, int owner_type,
							  uint32_t owner_id, const std::string &info,
							  const QuotaDatabase::Limits &limit) {
	static const char *owner_type_name[4] = {"User ", "Group", "Directory", "Unknown"};
	std::string line;

	if (owner_type < 0) {
		return;
	}

	fputs(owner_type_name[std::min(owner_type, 3)], stdout);
	fputs(" ", stdout);
	if (owner_type == (int)QuotaOwnerType::kInode) {
		fputs(path.c_str(), stdout);
		if (owner_id != path_inode) {
			fputs(info.c_str(), stdout);
		}
	} else {
		printf("%10" PRIu32, owner_id);
	}
	fputs(" ", stdout);

	quota_putc_plus_or_minus(limit[(int)QuotaRigor::kUsed][(int)QuotaResource::kSize],
	                         limit[(int)QuotaRigor::kSoft][(int)QuotaResource::kSize],
	                         limit[(int)QuotaRigor::kHard][(int)QuotaResource::kSize]);
	quota_putc_plus_or_minus(limit[(int)QuotaRigor::kUsed][(int)QuotaResource::kInodes],
	                         limit[(int)QuotaRigor::kSoft][(int)QuotaResource::kInodes],
	                         limit[(int)QuotaRigor::kHard][(int)QuotaResource::kInodes]);
	fputs(" ", stdout);
	print_number("", " ", limit[(int)QuotaRigor::kUsed][(int)QuotaResource::kSize], 0, 1, 1);
	print_number("", " ", limit[(int)QuotaRigor::kSoft][(int)QuotaResource::kSize], 0, 1, 1);
	print_number("", " ", limit[(int)QuotaRigor::kHard][(int)QuotaResource::kSize], 0, 1, 1);
	print_number("", " ", limit[(int)QuotaRigor::kUsed][(int)QuotaResource::kInodes], 0, 0, 1);
	print_number("", " ", limit[(int)QuotaRigor::kSoft][(int)QuotaResource::kInodes], 0, 0, 1);
	print_number("", " ", limit[(int)QuotaRigor::kHard][(int)QuotaResource::kInodes], 0, 0, 1);
	puts("");
}

static void quota_print_rep(const std::string &path, uint32_t path_inode,
							const std::vector<QuotaEntry> &quota_entries,
							const std::vector<std::string> &quota_info) {
	std::vector<std::size_t> ordering;

	ordering.resize(quota_entries.size());
	std::iota(ordering.begin(), ordering.end(), 0);

	std::sort(ordering.begin(), ordering.end(), [&quota_entries](std::size_t i1, std::size_t i2) {
		const QuotaEntry &e1 = quota_entries[i1];
		const QuotaEntry &e2 = quota_entries[i2];
		return std::make_tuple(e1.entryKey.owner.ownerType, e1.entryKey.owner.ownerId,
		                       e1.entryKey.rigor) < std::make_tuple(e2.entryKey.owner.ownerType,
		                                                            e2.entryKey.owner.ownerId,
		                                                            e2.entryKey.rigor);
	});

	char rpath[PATH_MAX + 1];
	std::string real_path;
	if (realpath(path.c_str(), rpath)) {
		real_path = rpath;
	} else {
		real_path = path;
	}

	puts(
	    "# User/Group ID/Directory; Bytes: current usage, soft limit, hard limit; "
	    "Inodes: current usage, soft limit, hard limit;");

	std::pair<int, uint32_t> prev_entry(-1, 0);
	QuotaDatabase::Limits limits_value{{{{0}}}};  // workaround for a bug in gcc 4.6
	std::string info;
	for (auto index : ordering) {
		const QuotaEntry &entry = quota_entries[index];
		auto type_with_id =
		    std::make_pair((int)entry.entryKey.owner.ownerType, entry.entryKey.owner.ownerId);

		if (type_with_id != prev_entry) {
			quota_print_entry(real_path, path_inode, prev_entry.first, prev_entry.second, info,
			                  limits_value);
			prev_entry = type_with_id;
			limits_value = QuotaDatabase::Limits{{{{0}}}};  // workaround for a bug in gcc 4.6
			info.clear();
		}
		if (index < quota_info.size()) {
			info = quota_info[index];
		}

		// Store only known values in limit table that quota_print_entry function can print.
		if ((unsigned)entry.entryKey.rigor < limits_value.size() &&
		    (unsigned)entry.entryKey.resource < limits_value[(int)entry.entryKey.rigor].size()) {
			limits_value[(int)entry.entryKey.rigor][(int)entry.entryKey.resource] = entry.limit;
		}
	}
	// print final entry
	quota_print_entry(real_path, path_inode, prev_entry.first, prev_entry.second, info,
	                  limits_value);
}

static int quota_rep(const std::string &path, std::vector<int> requested_uids,
					 std::vector<int> requested_gid, bool report_all, bool per_directory_quota) {
	std::vector<uint8_t> request;
	uint32_t uid = getuid();
	uint32_t gid = getgid();
	uint32_t message_id = 0;

	sassert((requested_uids.size() + requested_gid.size() > 0) ^
	        (report_all || per_directory_quota));

	uint32_t inode;
	int fd = open_master_conn(path.c_str(), &inode, nullptr, false);
	if (fd < 0) {
		return -1;
	}
	if (!per_directory_quota) {
		if (check_usage(quota_rep_usage, inode != 1, "Mount root path expected\n")) {
			return 1;
		}
	}

	if (report_all) {
		request = cltoma::fuseGetQuota::build(message_id, uid, gid);
	} else {
		std::vector<QuotaOwner> requested_entities;
		for (auto uid : requested_uids) {
			requested_entities.emplace_back(QuotaOwnerType::kUser, uid);
		}
		for (auto gid : requested_gid) {
			requested_entities.emplace_back(QuotaOwnerType::kGroup, gid);
		}
		if (per_directory_quota) {
			requested_entities.emplace_back(QuotaOwnerType::kInode, inode);
		}
		request = cltoma::fuseGetQuota::build(message_id, uid, gid, requested_entities);
	}

	try {
		auto response = ServerConnection::sendAndReceive(fd, request, LIZ_MATOCL_FUSE_GET_QUOTA);
		std::vector<QuotaEntry> quota_entries;
		std::vector<std::string> quota_info;
		PacketVersion version;
		deserializePacketVersionNoHeader(response, version);
		if (version == matocl::fuseGetQuota::kStatusPacketVersion) {
			uint8_t status;
			matocl::fuseGetQuota::deserialize(response, message_id, status);
			throw Exception(std::string(path) + ": failed", status);
		}
		matocl::fuseGetQuota::deserialize(response, message_id, quota_entries, quota_info);

		quota_print_rep(path, inode, quota_entries, quota_info);
	} catch (Exception &e) {
		fprintf(stderr, "%s\n", e.what());
		close_master_conn(1);
		return -1;
	}
	close_master_conn(0);
	return 0;
}

int quota_rep_run(int argc, char **argv) {
	std::vector<int> uid;
	std::vector<int> gid;
	bool reportAll = false;
	bool per_directory_quota = false;
	char *endptr = nullptr;
	std::string dir_path;
	int ch;

	while ((ch = getopt(argc, argv, "nhHdu:g:a")) != -1) {
		switch (ch) {
		case 'n':
			humode = 0;
			break;
		case 'h':
			humode = 1;
			break;
		case 'H':
			humode = 2;
			break;
		case 'u':
			uid.push_back(strtol(optarg, &endptr, 10));
			if (check_usage(quota_rep_usage, *endptr, "invalid uid: %s\n", optarg)) {
				return 1;
			}
			break;
		case 'g':
			gid.push_back(strtol(optarg, &endptr, 10));
			if (check_usage(quota_rep_usage, *endptr, "invalid gid: %s\n", optarg)) {
				return 1;
			}
			break;
		case 'd':
			per_directory_quota = true;
			break;
		case 'a':
			reportAll = true;
			break;
		default:
			fprintf(stderr, "invalid argument: %c", (char)ch);
			quota_rep_usage();
			return 1;
		}
	}
	if (check_usage(quota_rep_usage,
	            !((uid.size() + gid.size() != 0) ^ (reportAll || per_directory_quota)),
	            "provide either -a flag or uid/gid\n")) {
		return 1;
	}

	argc -= optind;
	argv += optind;

	if (check_usage(quota_rep_usage, argc != 1, "expected parameter: <mountpoint-root-path>\n")) {
		return 1;
	}
	dir_path = argv[0];

	return quota_rep(dir_path, uid, gid, reportAll, per_directory_quota);
}
