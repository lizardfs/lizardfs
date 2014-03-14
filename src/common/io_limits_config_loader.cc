#include "common/io_limits_config_loader.h"

#include <limits>

inline static bool streamReadFailed(const std::istream& stream) {
	return stream.eof() || stream.fail() || stream.bad();
}

void IoLimitsConfigLoader::load(std::istream&& stream) {
	limits_.clear();

	bool cgroupsInUse = false;
	while (!stream.eof()) {
		std::string command;
		std::string group;
		uint64_t limit;
		stream >> command;
		if (streamReadFailed(stream)) {
			if (stream.eof()) {
				break;
			}
			throw ParseException("Unexpected end of file.");
		}
		if (command == "subsystem") {
			stream >> subsystem_;
			if (streamReadFailed(stream)) {
				throw ParseException("Incorrect file format.");
			}
		} else if (command == "limit") {
			stream >> group >> limit;
			if (streamReadFailed(stream)) {
				throw ParseException("Incorrect file format.");
			} else if (limits_.find(group) != limits_.end()) {
				throw ParseException("Limit for group '" + group +
						"' specified more then once.");
			}
			limits_[group] = limit;
			cgroupsInUse |= (group != "unclassified");
		} else if (!command.empty() && command.front() == '#') {
			stream.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
		} else {
			throw ParseException("Unknown keyword '" + command + "'.");
		}
	}

	if (cgroupsInUse && subsystem_.empty()) {
		throw ParseException("Subsystem not specified.");
	}
}
