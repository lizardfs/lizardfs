#include <algorithm>
#include <iostream>
#include <map>

#include "common/cltoma_communication.h"
#include "common/exception.h"
#include "common/lizardfs_version.h"
#include "common/matocl_communication.h"
#include "common/message_receive_buffer.h"
#include "common/MFSCommunication.h"
#include "common/multi_buffer_writer.h"
#include "common/packet.h"
#include "common/sockets.h"
#include "tools/human_readable_format.h"

LIZARDFS_CREATE_EXCEPTION_CLASS(WrongUsageException, Exception);

static const std::string kPorcelainMode = "--porcelain";

int connect(const std::string& host, const std::string& port);
std::vector<uint8_t> sendAndReceive(int fd,
		const std::vector<uint8_t>& request,
		PacketHeader::Type expectedType);

class LizardFsProbeCommand {
public:
	virtual ~LizardFsProbeCommand() {}
	virtual std::string name() const = 0;
	virtual void usage() const = 0;
	virtual int run(const std::vector<std::string>& argv) const = 0;
};

struct ChunkserverEntry {
	NetworkAddress address;
	uint32_t version,  chunks, tdChunks, errorCount;
	uint64_t usedSpace, totalSpace, tdUsedSpace, tdTotalSpace;
};

inline uint32_t serializedSize(const ChunkserverEntry& entry) {
	return serializedSize(entry.version, entry.address,
			entry.usedSpace, entry.totalSpace, entry.chunks,
			entry.tdUsedSpace, entry.tdTotalSpace, entry.tdChunks, entry.errorCount);
}

inline void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer,
		ChunkserverEntry& value) {
	deserialize(source, bytesLeftInBuffer, value.version, value.address,
			value.usedSpace, value.totalSpace, value.chunks,
			value.tdUsedSpace, value.tdTotalSpace, value.tdChunks, value.errorCount);
}

class ChunkserversListInfoBase : public LizardFsProbeCommand {
protected:
	std::vector<ChunkserverEntry> getChunkserversList (
			const std::string& masterHost, const std::string& masterPort) const {
		std::vector<uint8_t> request, response;
		serializeMooseFsPacket(request, CLTOMA_CSERV_LIST);
		int fd = connect(masterHost, masterPort);
		try {
			response = sendAndReceive(fd, request, MATOCL_CSERV_LIST);
			tcpclose(fd);
		} catch (...) {
			tcpclose(fd);
			throw;
		}
		std::vector<ChunkserverEntry> result;
		while (!response.empty()) {
			result.push_back(ChunkserverEntry());
			deserialize(response, result.back());
			response.erase(response.begin(), response.begin() + serializedSize(result.back()));
		}
		return result;
	}
};

class ReadyChunkserversCount : public ChunkserversListInfoBase {
public:
	virtual std::string name() const {
		return "ready-chunkservers-count";
	}

	virtual void usage() const {
		std::cerr << name() << " <master ip> <master port>" << std::endl;
		std::cerr << "    prints number of chunkservers ready to be written to" << std::endl;
	}

	virtual int run(const std::vector<std::string>& argv) const {
		if (argv.size() != 2) {
			throw WrongUsageException("Expected exactly two arguments for " + name());
		}
		uint32_t readyChunkservers = 0;
		for (const ChunkserverEntry& cs : getChunkserversList(argv[0], argv[1])) {
			if (cs.totalSpace > 0) {
				++readyChunkservers;
			}
		}
		std::cout << readyChunkservers << std::endl;
		return 0;
	}
};

class ChunkserversList : public ChunkserversListInfoBase {
private:

public:
	virtual std::string name() const {
		return "list-chunkservers";
	}

	virtual void usage() const {
		std::cerr << name() << " <master ip> <master port> [" << kPorcelainMode << ']' << std::endl;
		std::cerr << "    prints information about all connected chunkservers\n" << std::endl;
		std::cerr << "        " << kPorcelainMode << std::endl;
		std::cerr << "    This argument makes the output parsing-friendly." << std::endl;
	}

	virtual int run(const std::vector<std::string>& argv) const {
		if (argv.size() < 2) {
			throw WrongUsageException("Expected <master ip> and <master port> for " + name());
		}
		if (argv.size() > 3) {
			throw WrongUsageException("Too many arguments for " + name());
		}
		if (argv.size() == 3 && argv[2] != kPorcelainMode) {
			throw WrongUsageException("Unexpected argument " + argv[2] + " for " + name());
		}
		std::vector<ChunkserverEntry> chunkservers = getChunkserversList(argv[0], argv[1]);
		bool porcelainMode = argv.back() == kPorcelainMode;

		// Printing
		if (!porcelainMode) {
			std::cout << "address\tversion\tchunks\tspace\tchunks to del\tto delete\terrors"
					<< std::endl;
		}
		for (const ChunkserverEntry& cs : chunkservers) {
			if (porcelainMode) {
				std::cout << cs.address.toString()
						<< ' ' << lizardfsVersionToString(cs.version)
						<< ' ' << cs.chunks
						<< ' ' << cs.usedSpace
						<< ' ' << cs.totalSpace
						<< ' ' << cs.tdChunks
						<< ' ' << cs.tdUsedSpace
						<< ' ' << cs.tdTotalSpace
						<< ' ' << cs.errorCount << std::endl;
			} else {
				std::cout << cs.address.toString()
						<< '\t' << lizardfsVersionToString(cs.version)
						<< '\t' << convertToSi(cs.chunks)
						<< '\t' << convertToIec(cs.usedSpace)
						<< '/' << convertToIec(cs.totalSpace)
						<< '\t' << convertToSi(cs.tdChunks)
						<< '\t' << convertToIec(cs.tdUsedSpace)
						<< '/' << convertToIec(cs.tdTotalSpace)
						<< '\t' << convertToSi(cs.errorCount) << std::endl;
			}
		}
		return 0;
	}
};

class ChunksHealth : public LizardFsProbeCommand {
public:
	virtual std::string name() const {
		return "chunks-health";
	}

	virtual void usage() const {
		std::cerr << name()  << " <master ip> <master port> [--<report>] [" << kPorcelainMode << "]"
				<< std::endl;
		std::cerr << "    Returns chunks health reports in the installation." << std::endl;
		std::cerr << "    Available reports:" << std::endl;
		std::cerr << "        " << kOptionAll << std::endl;
		std::cerr << "        " << kOptionAvailability << std::endl;
		std::cerr << "        " << kOptionReplication << std::endl;
		std::cerr << "        " << kOptionDeletion << std::endl;
		std::cerr << "    The default is " << kOptionAll << '.' << std::endl;
		std::cerr << "    In replication and deletion states, the column means the number of chunks"
				<< std::endl;
		std::cerr << "    with number of copies specified in the label to replicate/delete.\n"
				<< std::endl;
		std::cerr << "        " << kPorcelainMode << std::endl;
		std::cerr << "    This argument makes the output parsing-friendly." << std::endl;
	}

	virtual int run(const std::vector<std::string>& argv) const {
		if (argv.size() < 2 || argv.size() > 4) {
			throw WrongUsageException("Expected 2-4 arguments for " + name() + '\n');
		}
		bool isPorcelain = false;
		bool isAvailability = true, isReplication = true, isDeletion = true;
		if (argv.size() >= 3) {
			isPorcelain = argv.back() == kPorcelainMode;
			if (argv.size() == 4 || !isPorcelain) {
				if (argv.size() == 4 && !isPorcelain) {
					throw WrongUsageException("Wrong porcelain argument: " + argv[3]);
				}
				if (argv[2] != kOptionAll) {
					isAvailability = argv[2] == kOptionAvailability;
					isReplication = argv[2] == kOptionReplication;
					isDeletion = argv[2] == kOptionDeletion;
				}
				if (!(isAvailability | isReplication | isDeletion)) {
					throw WrongUsageException("Wrong argument: " + argv[2]);
				}
			}
		}

		std::vector<uint8_t> request, response;
		bool regularOnly = false;
		cltoma::xorChunksHealth::serialize(request, regularOnly);
		int fd = connect(argv[0], argv[1]);

		try {
			response = sendAndReceive(fd, request, LIZ_MATOCL_CHUNKS_HEALTH);
			tcpclose(fd);
		} catch (...) {
			tcpclose(fd);
			std::cerr << "Couldn't connect to master server" << std::endl;
			return -1;
		}

		ChunksAvailabilityState availability;
		ChunksReplicationState replication;
		matocl::xorChunksHealth::deserialize(response, regularOnly, availability, replication);
		if (regularOnly) {
			throw Exception("Incorrect response type received");
		}

		if (isAvailability) {
			printState(availability, isPorcelain);
		}
		if (isReplication) {
			printState(true, replication, isPorcelain);
		}
		if (isDeletion) {
			printState(false, replication, isPorcelain);
		}

		return 0;
	}

	static std::vector<uint8_t> collectGoals() {
		std::vector<uint8_t> goals = {0};
		for (uint8_t i = kMinOrdinaryGoal; i <= kMaxOrdinaryGoal; ++i) {
			goals.push_back(i);
		}
		for (ChunkType::XorLevel level = kMinXorLevel; level <= kMaxXorLevel; ++level) {
			goals.push_back(xorLevelToGoal(level));
		}
		return goals;
	}

	static std::map<uint8_t, std::string> createGoalNames() {
		std::map<uint8_t, std::string> goalNames;
		goalNames.insert({0, "0"});
		for (uint8_t goal = kMinOrdinaryGoal; goal <= kMaxOrdinaryGoal; ++goal) {
			goalNames.insert({goal, std::to_string(uint32_t(goal))});
		}
		for (ChunkType::XorLevel level = kMinXorLevel; level <= kMaxXorLevel; ++level) {
			uint8_t goal = xorLevelToGoal(level);
			goalNames.insert({goal, "xor" + std::to_string(level)});
		}

		return goalNames;
	}

private:
	static const std::vector<uint8_t> kGoals;
	static const std::map<uint8_t, std::string> kGoalNames;
	static const std::string kOptionAll;
	static const std::string kOptionAvailability;
	static const std::string kOptionReplication;
	static const std::string kOptionDeletion;

	void printState(const ChunksAvailabilityState& state, bool isPorcelain) const {
		if (isPorcelain) {
			for (uint8_t goal : kGoals) {
				std::cout << "AVA"
						<< ' ' << kGoalNames.at(goal)
						<< ' ' << state.safeChunks(goal)
						<< ' ' << state.endangeredChunks(goal)
						<< ' ' << state.lostChunks(goal) << std::endl;
			}
		} else {
			std::cout << "Chunks availability state:" << std::endl;
			std::cout << "\tGoal\tSafe\tUnsafe\tLost" << std::endl;
			for (uint8_t goal : kGoals) {
				if (state.safeChunks(goal) + state.endangeredChunks(goal)
						+ state.lostChunks(goal) == 0) {
					continue;
				}
				std::cout << '\t' << kGoalNames.at(goal)
						<< '\t' << print(state.safeChunks(goal))
						<< '\t' << print(state.endangeredChunks(goal))
						<< '\t' << print(state.lostChunks(goal)) << std::endl;
			}
			std::cout << std::endl;
		}
	}

	void printState(bool isReplication, const ChunksReplicationState& state,
			bool isPorcelain) const {
		if (isPorcelain) {
			for (uint8_t goal : kGoals) {
				std::cout << (isReplication ? "REP" : "DEL") << ' ' << kGoalNames.at(goal);
				for (uint32_t part = 0; part <= ChunksReplicationState::kMaxPartsCount; ++part) {
					isReplication
							? std::cout << ' ' << state.chunksToReplicate(goal, part)
							: std::cout << ' ' << state.chunksToDelete(goal, part);
				}
				std::cout << std::endl;
			}
		} else {
			isReplication ? std::cout << "Chunks replication state:" << std::endl
					: std::cout << "Chunks deletion state:" << std::endl;

			std::cout << "\tGoal";
			for (uint32_t i = 0; i <= ChunksReplicationState::kMaxPartsCount; ++i) {
				std::cout << '\t' << i;
			}
			std::cout << '+' << std::endl;

			for (uint8_t goal : kGoals) {
				std::string line = '\t' + kGoalNames.at(goal);
				uint64_t sum = 0;
				for (uint32_t part = 0; part <= ChunksReplicationState::kMaxPartsCount; ++part) {
					uint64_t chunksCount = isReplication ? state.chunksToReplicate(goal, part)
							: state.chunksToDelete(goal, part);
					sum += chunksCount;
					line += '\t' + print(chunksCount);
				}
				if (sum) {
					std::cout << line << std::endl;
				}
			}
			std::cout << std::endl;
		}
	}

	std::string print(uint64_t number) const {
		if (number == 0) {
			return "-";
		}
		return std::to_string(number);
	}
};

const std::vector<uint8_t> ChunksHealth::kGoals = ChunksHealth::collectGoals();
const std::map<uint8_t, std::string> ChunksHealth::kGoalNames = ChunksHealth::createGoalNames();
const std::string ChunksHealth::kOptionAll = "--all";
const std::string ChunksHealth::kOptionAvailability = "--availability";
const std::string ChunksHealth::kOptionReplication = "--replication";
const std::string ChunksHealth::kOptionDeletion = "--deletion";

int connect(const std::string& host, const std::string& port) {
	int fd = tcpsocket();
	if (fd < 0) {
		throw Exception("Can't create socket");
	}
	if (tcpstrconnect(fd, host.c_str(), port.c_str()) != 0) {
		tcpclose(fd);
		throw Exception("Can't connect to " + host + ":" + port);
	}
	return fd;
}

std::vector<uint8_t> sendAndReceive(int fd,
		const std::vector<uint8_t>& request,
		PacketHeader::Type expectedType) {
	// Send
	MultiBufferWriter writer;
	writer.addBufferToSend(request.data(), request.size());
	while (writer.hasDataToSend()) {
		ssize_t bytesWritten = writer.writeTo(fd);
		if (bytesWritten < 0) {
			throw Exception("Can't write data to socket");
		}
	}

	// Receive
	MessageReceiveBuffer reader(4 * 1024 * 1024);
	while (!reader.hasMessageData()) {
		ssize_t bytesRead = reader.readFrom(fd);
		if (bytesRead < 0) {
			throw Exception("Can't read data from socket");
		}
		if (reader.isMessageTooBig()) {
			throw Exception("Receive buffer overflow");
		}
	}
	if (reader.getMessageHeader().type != expectedType) {
		throw Exception("Received unexpected message");
	}

	uint32_t length = reader.getMessageHeader().length;
	return std::vector<uint8_t>(reader.getMessageData(), reader.getMessageData() + length);
}

int main(int argc, const char** argv) {
	std::vector<const LizardFsProbeCommand*> allCommands = {
			new ReadyChunkserversCount(),
			new ChunkserversList(),
			new ChunksHealth(),
	};

	try {
		if (argc < 2) {
			throw WrongUsageException("No command name provided");
		}
		std::string commandName(argv[1]);
		std::vector<std::string> arguments(argv + 2, argv + argc);
		for (auto command : allCommands) {
			if (command->name() == commandName) {
				return command->run(arguments);
			}
		}
		throw WrongUsageException("Unknown command " + commandName);
	} catch (WrongUsageException& ex) {
		std::cerr << ex.message() << std::endl;
		std::cerr << "Usage:\n";
		std::cerr << "    " << argv[0] << " COMMAND [ARGUMENTS...]\n\n";
		std::cerr << "Available COMMANDs:\n\n";
		for (auto command : allCommands) {
			command->usage();
			std::cerr << std::endl;
		}
		return 1;
	} catch (Exception& ex) {
		std::cerr << "Error: " << ex.what() << std::endl;
		return 1;
	}
}
