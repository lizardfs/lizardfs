#include "probe/ready_chunkservers_count_command.h"

#include <iostream>
#include <vector>

#include "probe/list_chunkservers_command.h"

std::string ReadyChunkserversCountCommand::name() const {
	return "ready-chunkservers-count";
}

void ReadyChunkserversCountCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>" << std::endl;
	std::cerr << "    prints number of chunkservers ready to be written to" << std::endl;
}

void ReadyChunkserversCountCommand::run(const std::vector<std::string>& argv) const {
	if (argv.size() != 2) {
		throw WrongUsageException("Expected exactly two arguments for " + name());
	}
	uint32_t readyChunkservers = 0;
	std::vector<ChunkserverEntry> chunkservers =
			ListChunkserversCommand::getChunkserversList(argv[0], argv[1]);
	for (const ChunkserverEntry& cs : chunkservers) {
		if (cs.totalSpace > 0) {
			++readyChunkservers;
		}
	}
	std::cout << readyChunkservers << std::endl;
}
