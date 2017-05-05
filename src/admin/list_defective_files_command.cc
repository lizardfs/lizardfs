/*
   Copyright 2017 Skytechnology sp. z o.o.

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
#include "admin/list_defective_files_command.h"

#include <iostream>

#include "admin/registered_admin_connection.h"
#include "protocol/cltoma.h"
#include "protocol/matocl.h"

static const uint64_t kDefaultEntriesLimit = 1000;

enum NodeErrorFlag {
	kChunkUnavailable = 1,
	kChunkUnderGoal   = 2,
	kStructureError   = 4,
	kAllNodeErrors    = 7
};

std::string ListDefectiveFilesCommand::name() const {
	return "list-defective-files";
}

void ListDefectiveFilesCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>" << std::endl;
	std::cerr << "    Lists files which currently have defective chunks" << std::endl;
}

LizardFsProbeCommand::SupportedOptions ListDefectiveFilesCommand::supportedOptions() const {
	return {
		{kPorcelainMode, kPorcelainModeDescription},
		{"--unavailable", "Print unavailable files"},
		{"--undergoal", "Print files with undergoal chunks"},
		{"--structure-error", "Print files/directories with structure error"},
		{"--limit=", "Limit maximum number of printed files"},
	};
}

static std::string flagToString(uint8_t flag) {
	std::string m;
	if (flag & kChunkUnavailable) {
		m += " unavailable";
	}
	if (flag & kChunkUnderGoal) {
		if (!m.empty()) {
			m += " |";
		}
		m += " undergoal";
	}
	if (flag & kStructureError) {
		if (!m.empty()) {
			m += " |";
		}
		m += " structure-error";
	}
	return m;
}

void ListDefectiveFilesCommand::run(const Options &options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}

	ServerConnection connection(options.argument(0), options.argument(1));
	uint8_t flags_set = 0;
	if (options.isSet("--unavailable")) {
		flags_set |= kChunkUnavailable;
	}
	if (options.isSet("--undergoal")) {
		flags_set |= kChunkUnderGoal;
	}
	if (options.isSet("--structure-error")) {
		flags_set |= kStructureError;
	}
	if (flags_set == 0) {
		flags_set = kAllNodeErrors; // if no option was set, use all flags as default
	}
	uint64_t entries_limit = options.getValue<uint64_t>("--limit", kDefaultEntriesLimit);
	uint64_t entries_left = entries_limit;
	uint64_t entry_index = 0;
	std::vector<DefectiveFileInfo> file_infos;
	bool porcelain = options.isSet(kPorcelainMode);
	if (!porcelain) {
		std::cout << "Files with error flag =" << flagToString(flags_set) << std::endl;
	}
	do {
		file_infos.clear();
		auto request = cltoma::listDefectiveFiles::build(flags_set, entry_index, entries_left);
		auto response = connection.sendAndReceive(request, LIZ_MATOCL_LIST_DEFECTIVE_FILES);
		matocl::listDefectiveFiles::deserialize(response, entry_index, file_infos);
		if (file_infos.size() > entries_left) {
			std::cerr << "Error: Received number of files larger than requested" << std::endl;
			exit(1);
		}
		if (entry_index == 0 && file_infos.empty() && entries_limit == entries_left && !porcelain) {
			std::cout << "  There are no files with given error flag" << std::endl;
			return;
		}
		if (porcelain) {
			for (const DefectiveFileInfo &info : file_infos) {
				std::cout << "\""<< info.file_name << "\" " << std::to_string(info.error_flags) << std::endl;
			}
		} else {
			for (const DefectiveFileInfo &info : file_infos) {
				std::cout << "  " << info.file_name << " -" << flagToString(info.error_flags) << std::endl;
			}
		}
		entries_left -= file_infos.size();
	} while (entry_index != 0 && entries_left > 0);
}
