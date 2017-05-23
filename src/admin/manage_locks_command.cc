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

#include "common/platform.h"
#include "admin/manage_locks_command.h"

#include <iomanip>
#include <iostream>

#include "admin/registered_admin_connection.h"
#include "common/lizardfs_version.h"
#include "common/server_connection.h"
#include "master/locks.h"
#include "protocol/cltoma.h"
#include "protocol/lock_info.h"
#include "protocol/matocl.h"

std::string ManageLocksCommand::name() const {
	return "manage-locks";
}

void ManageLocksCommand::usage() const {
	std::cerr << name() << " <master ip> <master port> [list/unlock] [flock/posix/all]" << std::endl;
	std::cerr << "    Manages file locks" << std::endl;
}

LizardFsProbeCommand::SupportedOptions ManageLocksCommand::supportedOptions() const {
	return {
		{kPorcelainMode, kPorcelainModeDescription},
		{"--active", "Print only active locks"},
		{"--pending", "Print only pending locks"},
		{"--inode=", "Specify an inode for operation"},
		{"--owner=", "Specify an owner for operation"},
		{"--sessionid=", "Specify a sessionid for operation"},
		{"--start=", "Specify a range start for operation"},
		{"--end=", "Specify a range end for operation"},
	};
}

static lzfs_locks::Type parseType(const std::string &str) {
	if (str == "flock") {
		return lzfs_locks::Type::kFlock;
	} else if (str == "posix") {
		return lzfs_locks::Type::kPosix;
	} else if (str == "all") {
		return lzfs_locks::Type::kAll;
	} else {
		throw WrongUsageException(str + " is not a valid lock type(flock, posix, all) ");
	}
}

static const char *lockTypeToString(uint16_t type) {
	if (type == static_cast<uint16_t>(FileLocks::Lock::Type::kShared)) {
		return "shared";
	} else if (type == static_cast<uint16_t>(FileLocks::Lock::Type::kExclusive)) {
		return "exclusive";
	} else {
		return "invalid";
	}
}

static void processUnlock(RegisteredAdminConnection &conn, const Options &options) {
	auto type = parseType(options.argument(3));

	if (!options.isSet("--inode")) {
		throw WrongUsageException("Inode number must be specified for unlocking");
	}
	if (options.isSet("--owner") != options.isSet("--sessionid")) {
		throw WrongUsageException("Both session id and owner must be specified for single unlock");
	}

	uint32_t inode = options.getValue<uint32_t>("--inode", 0);
	uint64_t owner = options.getValue<uint64_t>("--owner", 0);
	uint32_t sessionid = options.getValue<uint32_t>("--sessionid", 0);

	uint64_t start = options.getValue<uint64_t>("--start", 0);
	uint64_t end = options.getValue<uint64_t>("--end", std::numeric_limits<uint64_t>::max());

	MessageBuffer msg;
	if (owner) {
		msg = cltoma::manageLocksUnlock::build(type, inode, sessionid, owner, start, end);
	} else {
		msg = cltoma::manageLocksUnlock::build(type, inode);
	}
	auto response = conn.sendAndReceive(msg, LIZ_MATOCL_MANAGE_LOCKS_UNLOCK);

	uint8_t status;
	matocl::manageLocksUnlock::deserialize(response, status);
	std::cerr << "Status: " << lizardfs_error_string(status) << std::endl;
	if (status == LIZARDFS_ERROR_EPERM) {
		std::cerr << "This error might be caused by unmatched owner, sessionid, start or end"
			" parameters." << std::endl;
		exit(1);
	}
}

static void processListType(uint32_t inode, RegisteredAdminConnection &conn, const Options &options,
		lzfs_locks::Type type, bool pending) {
	const uint64_t kDataPortion = LIZ_CLTOMA_MANAGE_LOCKS_LIST_LIMIT;
	for (uint64_t begin = 0; true; begin += kDataPortion) {
		MessageBuffer msg;
		if (inode) {
			msg = cltoma::manageLocksList::build(inode, type, pending, begin, kDataPortion);
		} else {
			msg = cltoma::manageLocksList::build(type, pending, begin, kDataPortion);
		}
		auto response = conn.sendAndReceive(msg, LIZ_MATOCL_MANAGE_LOCKS_LIST);
		std::vector<lzfs_locks::Info> locks;
		matocl::manageLocksList::deserialize(response, locks);

		if (locks.size() > kDataPortion) {
			std::cerr << "Receive incorrect response from master";
			exit(1);
		}

		for (auto &lock : locks) {
			if (options.isSet(LizardFsProbeCommand::kPorcelainMode)) {
				std::cout << lock.inode
				<< " " << lock.owner
				<< " " << lock.sessionid
				<< " " << lockTypeToString(lock.type)
				<< std::endl;
			} else {
				std::cout << "Lock:"
				<< "\n\tinode: " << lock.inode
				<< "\n\towner: " << lock.owner
				<< "\n\tsessionid: " << lock.sessionid
				<< "\n\ttype:  " << lockTypeToString(lock.type)
				<< std::endl;
			}
		}

		if (locks.size() < kDataPortion) {
			break;
		}
	}
}

static void processList(RegisteredAdminConnection &conn, const Options &options) {
	auto type = parseType(options.argument(3));
	uint32_t inode = options.getValue("--inode", 0UL);
	if (type == lzfs_locks::Type::kFlock || type == lzfs_locks::Type::kAll) {
		if (options.isSet("--active") || !options.isSet("--pending")) {
			std::cout << "Active flocks:" << std::endl;
			processListType(inode, conn, options, lzfs_locks::Type::kFlock, false);
		}
		if (options.isSet("--pending") || !options.isSet("--active")) {
			std::cout << "Pending flocks:" << std::endl;
			processListType(inode, conn, options, lzfs_locks::Type::kFlock, true);
		}
	}
	if (type == lzfs_locks::Type::kPosix || type == lzfs_locks::Type::kAll) {
		if (options.isSet("--active") || !options.isSet("--pending")) {
			std::cout << "Active POSIX locks:" << std::endl;
			processListType(inode, conn, options, lzfs_locks::Type::kPosix, false);
		}
		if (options.isSet("--pending") || !options.isSet("--active")) {
			std::cout << "Pending POSIX locks:" << std::endl;
			processListType(inode, conn, options, lzfs_locks::Type::kPosix, true);
		}
	}
}

void ManageLocksCommand::run(const Options &options) const {
	if (options.arguments().size() != 4) {
		throw WrongUsageException("<master ip> <master port> [list/unlock] [flock/posix/all] for "
				+ name());
	}
	auto connection = RegisteredAdminConnection::create(options.argument(0), options.argument(1));

	if (options.argument(2) == "unlock") {
		processUnlock(*connection, options);
	} else if (options.argument(2) == "list") {
		processList(*connection, options);
	} else {
		throw WrongUsageException("Available management options: list unlock");
	}
}
