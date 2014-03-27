#include "probe/ready_chunkservers_count_command.h"

#include <iostream>
#include <vector>

#include "probe/list_chunkservers_command.h"

std::string ReadyChunkserversCountCommand::name() const {
	return "ready-chunkservers-count";
}

void ReadyChunkserversCountCommand::usage() const {
	std::cerr << name() << " <master ip> <master port>\n";
	std::cerr << "    Prints number of chunkservers ready to be written to.\n";
}

void ReadyChunkserversCountCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected exactly two arguments for " + name());
	}
	uint32_t readyChunkservers = 0;
	std::vector<ChunkserverEntry> chunkservers = ListChunkserversCommand::getChunkserversList(
			options.arguments(0), options.arguments(1));
	for (const ChunkserverEntry& cs : chunkservers) {
		if (cs.totalSpace > 0) {
			++readyChunkservers;
		}
	}
	std::cout << readyChunkservers << std::endl;
}
