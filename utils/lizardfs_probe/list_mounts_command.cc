#include "utils/lizardfs_probe/list_mounts_command.h"

#include <algorithm>
#include <iostream>
#include <vector>
#include <array>

#include "common/goal.h"
#include "common/human_readable_format.h"
#include "common/lizardfs_version.h"
#include "common/moosefs_vector.h"
#include "common/serializable_class.h"
#include "common/serialization.h"

typedef std::array<uint32_t, 16> OperationStats;

SERIALIZABLE_CLASS_BEGIN(MountEntry)
SERIALIZABLE_CLASS_BODY(MountEntry,
		uint32_t, sessionId,
		uint32_t, peerIp,
		uint32_t, version,
		std::string, info,
		std::string, path,
		uint8_t, flags,
		uint32_t, rootuid,
		uint32_t, rootgid,
		uint32_t, mapalluid,
		uint32_t, mapallgid,
		uint8_t, minGoal,
		uint8_t, maxGoal,
		uint32_t, minTrashTime,
		uint32_t, maxTrashTime,
		OperationStats, currentOpStats,
		OperationStats, lastHourOpStats)
		MountEntry() = default;
SERIALIZABLE_CLASS_END;

std::string ListMountsCommand::name() const {
	return "list-mounts";
}

void ListMountsCommand::usage() const {
	std::cerr << name() << " <master ip> <master port> [" << kPorcelainMode << '|'
			<< kVerboseMode << ']' << std::endl;
	std::cerr << "    prints information about all connected mounts\n" << std::endl;
	std::cerr << "        " << kPorcelainMode << std::endl;
	std::cerr << "    This argument makes the output parsing-friendly.\n" << std::endl;
	std::cerr << "        " << kVerboseMode << std::endl;
	std::cerr << "    Be a little more verbose and show goal and trash time limits.\n" << std::endl;
}

void ListMountsCommand::run(const std::vector<std::string>& argv) const {
	if (argv.size() < 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}
	bool verboseMode = false;
	bool porcelainMode = false;
	for (uint32_t i = 2; i < argv.size(); ++i) {
		if (argv[i] == kPorcelainMode) {
			porcelainMode = true;
		} else if (argv[i] == kVerboseMode) {
			verboseMode = true;
		} else {
			throw WrongUsageException("Unexpected argument " + argv[i] + " for " + name());
		}
	}

	MooseFSVector<MountEntry> mounts;
	std::vector<uint8_t> request, response;
	serializeMooseFsPacket(request, CLTOMA_SESSION_LIST, true);
	response = askMaster(request, argv[0], argv[1], MATOCL_SESSION_LIST);
	// There is uint16_t SESSION_STATS = 16 at the beginning of response
	uint16_t dummy;
	deserializeAllMooseFsPacketDataNoHeader(response, dummy, mounts);

	std::sort(mounts.begin(), mounts.end(),
			[](const MountEntry& a, const MountEntry& b) -> bool
			{ return a.sessionId_ < b.sessionId_; });

	for (const MountEntry& mount : mounts) {
		bool readonly = mount.flags_ & SESFLAG_READONLY;
		bool restrictedIp = !(mount.flags_ & SESFLAG_DYNAMICIP);
		bool ignoregid = mount.flags_ & SESFLAG_IGNOREGID;
		bool canChangeQuota = mount.flags_ & SESFLAG_CANCHANGEQUOTA;
		bool mapAll = mount.flags_ & SESFLAG_MAPALL;
		bool shouldPrintGoal = isOrdinaryGoal(mount.minGoal_) && isOrdinaryGoal(mount.maxGoal_);
		bool shouldPrintTrashTime = mount.minTrashTime_ < mount.maxTrashTime_
				&& (mount.minTrashTime_ != 0 || mount.maxTrashTime_ != 0xFFFFFFFF);

		if (!porcelainMode) {
			std::cout << "session " << mount.sessionId_ << ": " << '\n'
					<< "\tip: " << ipToString(mount.peerIp_) << '\n'
					<< "\tmount point: " << mount.info_ << '\n'
					<< "\tversion: " << lizardfsVersionToString(mount.version_) << '\n'
					<< "\troot dir: " << mount.path_ << '\n'
					<< "\troot uid: " << mount.rootuid_ << '\n'
					<< "\troot gid: " << mount.rootgid_ << '\n'
					<< "\tusers uid: " << mount.mapalluid_ << '\n'
					<< "\tusers gid: " << mount.mapallgid_ << '\n'
					<< "\tread only: " << (readonly ? "yes" : "no") << '\n'
					<< "\trestricted ip: " << (restrictedIp ? "yes" : "no") << '\n'
					<< "\tignore gid: " << (ignoregid ? "yes" : "no") << '\n'
					<< "\tcan change quota: " << (canChangeQuota ? "yes" : "no") << '\n'
					<< "\tmap all users: " << (mapAll ? "yes" : "no") << std::endl;

			if (verboseMode) {
				if (shouldPrintGoal) {
					std::cout << "\tmin goal: " << static_cast<uint32_t>(mount.minGoal_) << std::endl
							<< "\tmax goal: " << static_cast<uint32_t>(mount.maxGoal_) << std::endl;
				} else {
					std::cout << "\tmin goal: -\n\tmax goal: -" << std::endl;
				}

				if (shouldPrintTrashTime) {
					std::cout << "\tmin trash time:" << mount.minTrashTime_ << std::endl
							<< "\tmax trash time: " << mount.maxTrashTime_ << std::endl;
				} else {
					std::cout << "\tmin trash time: -\n\tmax trash time: -" << std::endl;
				}
			}
		} else {
			std::cout << mount.sessionId_
					<< ' ' << ipToString(mount.peerIp_)
					<< ' ' << mount.info_
					<< ' ' << lizardfsVersionToString(mount.version_)
					<< ' ' << mount.path_
					<< ' ' << mount.rootuid_
					<< ' ' << mount.rootgid_
					<< ' ' << mount.mapalluid_
					<< ' ' << mount.mapallgid_
					<< ' ' << (readonly ? "yes" : "no")
					<< ' ' << (restrictedIp ? "yes" : "no")
					<< ' ' << (ignoregid ? "yes" : "no")
					<< ' ' << (canChangeQuota ? "yes" : "no")
					<< ' ' << (mapAll ? "yes" : "no");
			if (verboseMode) {
				if (shouldPrintGoal) {
					std::cout << ' ' << static_cast<uint32_t>(mount.minGoal_)
							<< ' ' << static_cast<uint32_t>(mount.maxGoal_);
				} else {
					std::cout << " - -";
				}

				if (shouldPrintTrashTime) {
					std::cout << ' ' << mount.minTrashTime_
							<< ' ' << mount.maxTrashTime_;
				} else {
					std::cout << " - -";
				}
			}
			std::cout << std::endl;
		}
	}
}
