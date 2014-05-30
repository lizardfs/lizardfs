#include "config.h"
#include "mount/io_limit_group.h"

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

static void skipHierarchy(std::istream& is) {
	char x;
	do {
		is >> x;
	} while (x != ':');
}

static bool searchSubsystems(std::istream& is, const std::string& subsystem) {
	const int length = subsystem.length();
	int matched = 0;
	char x;
	while (1) {
		is >> x;
		if (matched == length && (x == ',' || x == ':')) {
			while (x != ':') {
				is >> x;
			}
			return true;
		}
		if (matched == length || x != subsystem[matched]) {
			while (x != ',' && x != ':') {
				is >> x;
			}
			if (x == ':') {
				return false;
			}
			matched = 0;
			continue;
		}
		matched++;
	}
}

IoLimitGroupId getIoLimitGroupId(std::istream& input, const std::string& subsystem) {
	try {
		for (std::string line; std::getline(input, line); ) {
			try {
				std::stringstream ss(line);
				ss.exceptions(std::stringstream::eofbit);
				skipHierarchy(ss);
				if (searchSubsystems(ss, subsystem)) {
					ss.exceptions(std::stringstream::goodbit);
					std::string groupId;
					std::getline(ss, groupId);
					return groupId;
				}
			} catch (std::ios_base::failure&) {
				throw GetIoLimitGroupIdException("Parse error");
			}
		}
	} catch (std::ios_base::failure&) {
		if (!input.eof()) {
			throw;
		}
	}
	throw GetIoLimitGroupIdException("Can't find subsystem '" + subsystem + "'");
}

IoLimitGroupId getIoLimitGroupId(const pid_t pid, const std::string& subsystem) {
	char filename[32];
	sprintf(filename, "/proc/%u/cgroup", pid);
	try {
		std::ifstream ifs;
		ifs.exceptions(std::ifstream::failbit | std::ifstream::badbit);
		ifs.open(filename);
		return getIoLimitGroupId(ifs, subsystem);
	} catch (std::ios_base::failure& ex) {
		throw GetIoLimitGroupIdException(
				"Error reading '" + std::string(filename) + ": " + ex.what());
	}
}

IoLimitGroupId getIoLimitGroupIdNoExcept(const pid_t pid, const std::string& subsystem) {
	try {
		return getIoLimitGroupId(pid, subsystem);
	} catch (GetIoLimitGroupIdException&) {
		return kUnclassified;
	}
}
