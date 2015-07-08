/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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
#include "admin/info_command.h"

#include <iostream>

#include "common/human_readable_format.h"
#include "common/lizardfs_statistics.h"
#include "common/lizardfs_version.h"
#include "common/server_connection.h"

std::string InfoCommand::name() const {
	return "info";
}

LizardFsProbeCommand::SupportedOptions InfoCommand::supportedOptions() const {
	return {
		{kPorcelainMode, kPorcelainModeDescription},
	};
}

void InfoCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>\n";
	std::cerr << "    Prints statistics concerning the LizardFS installation.\n";
}

void InfoCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name());
	}

	ServerConnection connection(options.argument(0), options.argument(1));
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
				<< "Memory usage:\t" << convertToIec(info.memoryUsage) << "B\n"
				<< "Total space:\t" << convertToIec(info.totalSpace) << "B\n"
				<< "Available space:\t" << convertToIec(info.availableSpace) << "B\n"
				<< "Trash space:\t" << convertToIec(info.trashSpace) << "B\n"
				<< "Trash files:\t" << info.trashNodes << '\n'
				<< "Reserved space:\t" << convertToIec(info.reservedSpace) << "B\n"
				<< "Reserved files:\t" << info.reservedNodes << '\n'
				<< "FS objects:\t" << info.allNodes << '\n'
				<< "Directories:\t" << info.dirNodes << '\n'
				<< "Files:\t" << info.fileNodes << '\n'
				<< "Chunks:\t" << info.chunks << '\n'
				<< "Chunk copies:\t" << info.chunkCopies << '\n'
				<< "Regular copies:\t" << info.regularCopies << std::endl;
	}
}
