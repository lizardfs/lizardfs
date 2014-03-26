#include "probe/lizardfs_info_command.h"

#include <iostream>

#include "common/human_readable_format.h"
#include "common/lizardfs_statistics.h"
#include "common/lizardfs_version.h"
#include "common/packet.h"
#include "probe/options.h"
#include "probe/server_connection.h"

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
	Options options({kPorcelainMode}, argv);
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}

	ServerConnection connection(options.arguments(0), options.arguments(1));
	std::vector<uint8_t> request, response;
	serializeMooseFsPacket(request, CLTOMA_INFO);
	response = connection.sendAndReceive(request, MATOCL_INFO);
	LizardFsStatistics info;
	deserializeAllMooseFsPacketDataNoHeader(response, info);
	if (options.isSet(kPorcelainMode)) {
		std::cout << lizardfsVersionToString(info.version)
				<< ' ' << info.memoryUsage
				<< ' ' << info.totalSpace
				<< ' ' << info.availableSpace
				<< ' ' << info.trashSpace
				<< ' ' << info.trashNodes
				<< ' ' << info.reservedSpace
				<< ' ' << info.reservedNodes
				<< ' ' << info.allNodes
				<< ' ' << info.dirNodes
				<< ' ' << info.fileNodes
				<< ' ' << info.chunks
				<< ' ' << info.chunkCopies
				<< ' ' << info.regularCopies
				<< std::endl;
	} else {
		std::cout << "LizardFS v" << lizardfsVersionToString(info.version) << '\n'
				<< "Memory usage:\t" << convertToIec(info.memoryUsage) << '\n'
				<< "Total space:\t" << convertToIec(info.totalSpace) << '\n'
				<< "Available space:\t" << convertToIec(info.availableSpace) << '\n'
				<< "Trash space:\t" << convertToIec(info.trashSpace) << '\n'
				<< "Trash files:\t" << info.trashNodes << '\n'
				<< "Reserved space:\t" << convertToIec(info.reservedSpace) << '\n'
				<< "Reserved files:\t" << info.reservedNodes << '\n'
				<< "FS objects:\t" << info.allNodes << '\n'
				<< "Directories:\t" << info.dirNodes << '\n'
				<< "Files:\t" << info.fileNodes << '\n'
				<< "Chunks:\t" << info.chunks << '\n'
				<< "Chunk copies:\t" << info.chunkCopies << '\n'
				<< "Regular copies:\t" << info.regularCopies << std::endl;
	}
}
