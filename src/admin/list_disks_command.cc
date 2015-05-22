#include "common/platform.h"
#include "admin/list_disks_command.h"

#include <iostream>

#include "common/disk_info.h"
#include "common/human_readable_format.h"
#include "common/lizardfs_version.h"
#include "common/moosefs_vector.h"
#include "common/server_connection.h"
#include "admin/list_chunkservers_command.h"

static std::string boolToYesNoString(bool value) {
	return (value ? "yes" : "no");
}

static void printBps(uint64_t bytes, uint64_t usec) {
	if (usec > 0) {
		std::cout << convertToIec(bytes)
				<< "B (" << bpsToString(bytes, usec) << ")";
	} else {
		std::cout << "-";
	}
}

static void printOperationTime(uint32_t time) {
	if (time > 0) {
		std::cout << time << "us";
	} else {
		std::cout << '-';
	}
}

static void printOperationCount(uint32_t count) {
	if (count > 0) {
		std::cout << convertToSi(count);
	} else {
		std::cout << '-';
	}
}

static void printReadTransfer(const HddStatistics& stats) {
	printBps(stats.rbytes, stats.usecreadsum);
}

static void printWriteTransfer(const HddStatistics& stats) {
	printBps(stats.wbytes, stats.usecwritesum);
}

static void printMaxReadTime(const HddStatistics& stats) {
	printOperationTime(stats.usecreadmax);
}

static void printMaxWriteTime(const HddStatistics& stats) {
	printOperationTime(stats.usecwritemax);
}

static void printMaxFsyncTime(const HddStatistics& stats) {
	printOperationTime(stats.usecfsyncmax);
}

static void printReadOperationCount(const HddStatistics& stats) {
	printOperationCount(stats.rops);
}

static void printWriteOperationCount(const HddStatistics& stats) {
	printOperationCount(stats.wops);
}

static void printFsyncOperationCount(const HddStatistics& stats) {
	printOperationCount(stats.fsyncops);
}

static void printStats(const std::string& header, const HddStatistics* stats[3],
		void (*printer)(const HddStatistics&)) {
	std::cout << '\t' << header << ":";
	for (int i = 0; i < 3; ++i) {
		std::cout << '\t';
		printer(*stats[i]);
	}
	std::cout << std::endl;
}

static void printPorcelainStats(const HddStatistics& stats) {
	std::cout << stats.rbytes
			<< ' ' << stats.wbytes
			<< ' ' << stats.usecreadmax
			<< ' ' << stats.usecwritemax
			<< ' ' << stats.usecfsyncmax
			<< ' ' << stats.rops
			<< ' ' << stats.wops
			<< ' ' << stats.fsyncops;
}

static void printPorcelainMode(const ChunkserverListEntry& cs, const MooseFSVector<DiskInfo>& disks,
		bool verbose) {
	for (const DiskInfo& disk : disks) {
		std::cout << NetworkAddress(cs.servip, cs.servport).toString()
				<< ' ' << disk.path
				<< ' ' << (disk.flags & DiskInfo::kToDeleteFlagMask ? "yes" : "no")
				<< ' ' << (disk.flags & DiskInfo::kDamagedFlagMask ? "yes" : "no")
				<< ' ' << (disk.flags & DiskInfo::kScanInProgressFlagMask ? "yes" : "no")
				<< ' ' << disk.errorChunkId
				<< ' ' << disk.errorTimeStamp
				<< ' ' << disk.total
				<< ' ' << disk.used
				<< ' ' << disk.chunksCount;
		if (verbose) {
			std::cout << ' ';
			printPorcelainStats(disk.lastMinuteStats);
			std::cout << ' ';
			printPorcelainStats(disk.lastHourStats);
			std::cout << ' ';
			printPorcelainStats(disk.lastDayStats);
		}
		std::cout << std::endl;
	}
}

static void printNormalMode(const ChunkserverListEntry& cs, const MooseFSVector<DiskInfo>& disks,
		bool verbose) {
	for (const DiskInfo& disk : disks) {
		std::string lastError;
		if (disk.errorChunkId == 0 && disk.errorTimeStamp == 0) {
			lastError = "no errors";
		} else {
			std::ostringstream ss;
			ss << "chunk " << disk.errorChunkId
					<< " (" << timeToString(disk.errorTimeStamp) << ')';
			lastError = ss.str();
		}
		std::cout << NetworkAddress(cs.servip, cs.servport).toString() << ":" << disk.path << '\n'
				<< "\tto delete: "
				<< boolToYesNoString(disk.flags & DiskInfo::kToDeleteFlagMask) << '\n'
				<< "\tdamaged: "
				<< boolToYesNoString(disk.flags & DiskInfo::kDamagedFlagMask) << '\n'
				<< "\tscanning: "
				<< boolToYesNoString(disk.flags & DiskInfo::kScanInProgressFlagMask) << '\n'
				<< "\tlast error: " << lastError << '\n'
				<< "\ttotal space: " << convertToIec(disk.total) << "B\n"
				<< "\tused space: " << convertToIec(disk.used) << "B\n"
				<< "\tchunks: " << convertToSi(disk.chunksCount) << std::endl;
		if (verbose) {
			const HddStatistics* stats[3] = {
					&disk.lastMinuteStats,
					&disk.lastHourStats,
					&disk.lastDayStats
			};
			std::cout << "\t\tminute\thour\tday\n";
			printStats("read bytes", stats, printReadTransfer);
			printStats("written bytes", stats, printWriteTransfer);
			printStats("max read time", stats, printMaxReadTime);
			printStats("max write time", stats, printMaxWriteTime);
			printStats("max fsync time", stats, printMaxFsyncTime);
			printStats("read ops", stats, printReadOperationCount);
			printStats("write ops", stats, printWriteOperationCount);
			printStats("fsync ops", stats, printFsyncOperationCount);
		}
	}
}

std::string ListDisksCommand::name() const {
	return "list-disks";
}

LizardFsProbeCommand::SupportedOptions ListDisksCommand::supportedOptions() const {
	return {
		{kPorcelainMode, kPorcelainModeDescription},
		{kVerboseMode,   "Be a little more verbose and show operations statistics."},
	};
}

void ListDisksCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>\n";
	std::cerr << "    Prints information about all connected chunkservers.\n";
}

void ListDisksCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}
	auto chunkservers = ListChunkserversCommand::getChunkserversList(
			options.argument(0), options.argument(1));
	for (const auto& cs : chunkservers) {
		if (cs.version == kDisconnectedChunkserverVersion) {
			continue; // skip disconnected chunkservers -- these surely won't respond
		}
		std::vector<uint8_t> request, response;
		serializeMooseFsPacket(request, CLTOCS_HDD_LIST_V2);
		ServerConnection connection(NetworkAddress(cs.servip, cs.servport));
		response = connection.sendAndReceive(request, CSTOCL_HDD_LIST_V2);
		MooseFSVector<DiskInfo> disks;
		deserializeAllMooseFsPacketDataNoHeader(response, disks);
		if (options.isSet(kPorcelainMode)) {
			printPorcelainMode(cs, disks, options.isSet(kVerboseMode));
		} else {
			printNormalMode(cs, disks, options.isSet(kVerboseMode));
		}
	}
}
