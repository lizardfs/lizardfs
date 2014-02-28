#include "utils/lizardfs_probe/lizardfs_info_command.h"

#include <iostream>

#include "common/human_readable_format.h"
#include "common/lizardfs_statistics.h"
#include "common/lizardfs_version.h"

std::string LizardFsInfoCommand::name() const {
	return "info";
}

void LizardFsInfoCommand::usage() const {
	std::cerr << name() << " <master ip> <master port> [" << kPorcelainMode << ']' << std::endl;
	std::cerr << "    prints statistics concerning the LizardFS installation\n" << std::endl;
	std::cerr << "        " << kPorcelainMode << std::endl;
	std::cerr << "    This argument makes the output parsing-friendly." << std::endl;
}

void LizardFsInfoCommand::run(const std::vector<std::string>& argv) const {
	if (argv.size() < 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}
	if (argv.size() > 3) {
		throw WrongUsageException("Too many arguments for " + name());
	}
	if (argv.size() == 3 && argv[2] != kPorcelainMode) {
		throw WrongUsageException("Unexpected argument " + argv[2] + " for " + name());
	}
	bool porcelainMode = argv.back() == kPorcelainMode;
	std::vector<uint8_t> request, response;
	serializeMooseFsPacket(request, CLTOMA_INFO);
	response = askMaster(request, argv[0], argv[1], MATOCL_INFO);
	LizardFsStatistics info;
	deserializeAllMooseFsPacketDataNoHeader(response, info);
	if (porcelainMode) {
		std::cout << lizardfsVersionToString(info.version_)
				<< ' ' << info.memoryUsage_
				<< ' ' << info.totalSpace_
				<< ' ' << info.availableSpace_
				<< ' ' << info.trashSpace_
				<< ' ' << info.trashNodes_
				<< ' ' << info.reservedSpace_
				<< ' ' << info.reservedNodes_
				<< ' ' << info.allNodes_
				<< ' ' << info.dirNodes_
				<< ' ' << info.fileNodes_
				<< ' ' << info.chunks_
				<< ' ' << info.chunkCopies_
				<< ' ' << info.regularCopies_
				<< std::endl;
	} else {
		std::cout << "LizardFS v" << lizardfsVersionToString(info.version_) << '\n'
				<< "Memory usage:\t" << convertToIec(info.memoryUsage_) << '\n'
				<< "Total space:\t" << convertToIec(info.totalSpace_) << '\n'
				<< "Available space:\t" << convertToIec(info.availableSpace_) << '\n'
				<< "Trash space:\t" << convertToIec(info.trashSpace_) << '\n'
				<< "Trash files:\t" << info.trashNodes_ << '\n'
				<< "Reserved space:\t" << convertToIec(info.reservedSpace_) << '\n'
				<< "Reserved files:\t" << info.reservedNodes_ << '\n'
				<< "FS objects:\t" << info.allNodes_ << '\n'
				<< "Directories:\t" << info.dirNodes_ << '\n'
				<< "Files:\t" << info.fileNodes_ << '\n'
				<< "Chunks:\t" << info.chunks_ << '\n'
				<< "Chunk copies:\t" << info.chunkCopies_ << '\n'
				<< "Regular copies:\t" << info.regularCopies_ << std::endl;
	}
}
