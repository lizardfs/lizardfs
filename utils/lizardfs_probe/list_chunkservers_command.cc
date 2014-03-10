#include "utils/lizardfs_probe/list_chunkservers_command.h"

#include <iostream>
#include <vector>

#include "common/human_readable_format.h"
#include "common/lizardfs_version.h"

std::string ListChunkserversCommand::name() const {
	return "list-chunkservers";
}

void ListChunkserversCommand::usage() const {
	std::cerr << name() << " <master ip> <master port> [" << kPorcelainMode << ']' << std::endl;
	std::cerr << "    prints information about all connected chunkservers\n" << std::endl;
	std::cerr << "        " << kPorcelainMode << std::endl;
	std::cerr << "    This argument makes the output parsing-friendly." << std::endl;
}

void ListChunkserversCommand::run(const std::vector<std::string>& argv) const {
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
	std::vector<ChunkserverEntry> chunkservers = getChunkserversList(argv[0], argv[1]);

	if (!porcelainMode) {
		std::cout << "address\tversion\tchunks\tspace\tchunks to del\tto delete\terrors"
				<< std::endl;
	}
	for (const ChunkserverEntry& cs : chunkservers) {
		if (porcelainMode) {
			std::cout << cs.address_.toString()
					<< ' ' << lizardfsVersionToString(cs.version_)
					<< ' ' << cs.chunks_
					<< ' ' << cs.usedSpace_
					<< ' ' << cs.totalSpace_
					<< ' ' << cs.tdChunks_
					<< ' ' << cs.tdUsedSpace_
					<< ' ' << cs.tdTotalSpace_
					<< ' ' << cs.errorCount_ << std::endl;
		} else {
			std::cout << cs.address_.toString()
					<< '\t' << lizardfsVersionToString(cs.version_)
					<< '\t' << convertToSi(cs.chunks_)
					<< '\t' << convertToIec(cs.usedSpace_)
					<< '/' << convertToIec(cs.totalSpace_)
					<< '\t' << convertToSi(cs.tdChunks_)
					<< '\t' << convertToIec(cs.tdUsedSpace_)
					<< '/' << convertToIec(cs.tdTotalSpace_)
					<< '\t' << convertToSi(cs.errorCount_) << std::endl;
		}
	}
}

std::vector<ChunkserverEntry> ListChunkserversCommand::getChunkserversList (
		const std::string& masterHost, const std::string& masterPort) {
	std::vector<uint8_t> request, response;
	serializeMooseFsPacket(request, CLTOMA_CSERV_LIST);
	response = askMaster(request, masterHost, masterPort, MATOCL_CSERV_LIST);
	std::vector<ChunkserverEntry> result;
	while (!response.empty()) {
		result.push_back(ChunkserverEntry());
		deserialize(response, result.back());
		response.erase(response.begin(), response.begin() + serializedSize(result.back()));
	}
	return result;
}
