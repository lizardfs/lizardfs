#include "utils/lizardfs_probe/list_mounts_command.h"

#include <algorithm>
#include <iostream>
#include <vector>
#include <array>

#include "common/goal.h"
#include "common/human_readable_format.h"
#include "common/lizardfs_version.h"
#include "common/moosefs_vector.h"
#include "common/packet.h"
#include "common/serializable_class.h"
#include "common/serialization.h"
#include "utils/lizardfs_probe/options.h"
#include "utils/lizardfs_probe/server_connection.h"

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
	Options options({kVerboseMode, kPorcelainMode}, argv);
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}

	ServerConnection connection(options.arguments(0), options.arguments(1));
	MooseFSVector<MountEntry> mounts;
	std::vector<uint8_t> request, response;
	serializeMooseFsPacket(request, CLTOMA_SESSION_LIST, true);
	response = connection.sendAndReceive(request, MATOCL_SESSION_LIST);
	// There is uint16_t SESSION_STATS = 16 at the beginning of response
	uint16_t dummy;
	deserializeAllMooseFsPacketDataNoHeader(response, dummy, mounts);

	std::sort(mounts.begin(), mounts.end(),
			[](const MountEntry& a, const MountEntry& b) -> bool
			{ return a.sessionId < b.sessionId; });

	for (const MountEntry& mount : mounts) {
		bool readonly = mount.flags & SESFLAG_READONLY;
		bool restrictedIp = !(mount.flags & SESFLAG_DYNAMICIP);
		bool ignoregid = mount.flags & SESFLAG_IGNOREGID;
		bool canChangeQuota = mount.flags & SESFLAG_CANCHANGEQUOTA;
		bool mapAll = mount.flags & SESFLAG_MAPALL;
		bool shouldPrintGoal = isOrdinaryGoal(mount.minGoal) && isOrdinaryGoal(mount.maxGoal);
		bool shouldPrintTrashTime = mount.minTrashTime < mount.maxTrashTime
				&& (mount.minTrashTime != 0 || mount.maxTrashTime != 0xFFFFFFFF);

		if (!options.isSet(kPorcelainMode)) {
			std::cout << "session " << mount.sessionId << ": " << '\n'
					<< "\tip: " << ipToString(mount.peerIp) << '\n'
					<< "\tmount point: " << mount.info << '\n'
					<< "\tversion: " << lizardfsVersionToString(mount.version) << '\n'
					<< "\troot dir: " << mount.path << '\n'
					<< "\troot uid: " << mount.rootuid << '\n'
					<< "\troot gid: " << mount.rootgid << '\n'
					<< "\tusers uid: " << mount.mapalluid << '\n'
					<< "\tusers gid: " << mount.mapallgid << '\n'
					<< "\tread only: " << (readonly ? "yes" : "no") << '\n'
					<< "\trestricted ip: " << (restrictedIp ? "yes" : "no") << '\n'
					<< "\tignore gid: " << (ignoregid ? "yes" : "no") << '\n'
					<< "\tcan change quota: " << (canChangeQuota ? "yes" : "no") << '\n'
					<< "\tmap all users: " << (mapAll ? "yes" : "no") << std::endl;

			if (options.isSet(kVerboseMode)) {
				if (shouldPrintGoal) {
					std::cout << "\tmin goal: " << static_cast<uint32_t>(mount.minGoal) << std::endl
							<< "\tmax goal: " << static_cast<uint32_t>(mount.maxGoal) << std::endl;
				} else {
					std::cout << "\tmin goal: -\n\tmax goal: -" << std::endl;
				}

				if (shouldPrintTrashTime) {
					std::cout << "\tmin trash time:" << mount.minTrashTime << std::endl
							<< "\tmax trash time: " << mount.maxTrashTime << std::endl;
				} else {
					std::cout << "\tmin trash time: -\n\tmax trash time: -" << std::endl;
				}
			}
		} else {
			std::cout << mount.sessionId
					<< ' ' << ipToString(mount.peerIp)
					<< ' ' << mount.info
					<< ' ' << lizardfsVersionToString(mount.version)
					<< ' ' << mount.path
					<< ' ' << mount.rootuid
					<< ' ' << mount.rootgid
					<< ' ' << mount.mapalluid
					<< ' ' << mount.mapallgid
					<< ' ' << (readonly ? "yes" : "no")
					<< ' ' << (restrictedIp ? "yes" : "no")
					<< ' ' << (ignoregid ? "yes" : "no")
					<< ' ' << (canChangeQuota ? "yes" : "no")
					<< ' ' << (mapAll ? "yes" : "no");
			if (options.isSet(kVerboseMode)) {
				if (shouldPrintGoal) {
					std::cout << ' ' << static_cast<uint32_t>(mount.minGoal)
							<< ' ' << static_cast<uint32_t>(mount.maxGoal);
				} else {
					std::cout << " - -";
				}

				if (shouldPrintTrashTime) {
					std::cout << ' ' << mount.minTrashTime
							<< ' ' << mount.maxTrashTime;
				} else {
					std::cout << " - -";
				}
			}
			std::cout << std::endl;
		}
	}
}
