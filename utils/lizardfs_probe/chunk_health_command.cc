#include "utils/lizardfs_probe/chunk_health_command.h"

#include <iostream>

#include "common/cltoma_communication.h"
#include "common/matocl_communication.h"

const std::vector<uint8_t> ChunksHealthCommand::kGoals =
		ChunksHealthCommand::collectGoals();
const std::map<uint8_t, std::string> ChunksHealthCommand::kGoalNames =
		ChunksHealthCommand::createGoalNames();
const std::string ChunksHealthCommand::kOptionAll = "--all";
const std::string ChunksHealthCommand::kOptionAvailability = "--availability";
const std::string ChunksHealthCommand::kOptionReplication = "--replication";
const std::string ChunksHealthCommand::kOptionDeletion = "--deletion";

std::string ChunksHealthCommand::name() const {
	return "chunks-health";
}

void ChunksHealthCommand::usage() const {
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

void ChunksHealthCommand::run(const std::vector<std::string>& argv) const {
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
	response = askMaster(request, argv[0], argv[1], LIZ_MATOCL_CHUNKS_HEALTH);
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
}

std::vector<uint8_t> ChunksHealthCommand::collectGoals() {
	std::vector<uint8_t> goals = {0};
	for (uint8_t i = kMinOrdinaryGoal; i <= kMaxOrdinaryGoal; ++i) {
		goals.push_back(i);
	}
	for (ChunkType::XorLevel level = kMinXorLevel; level <= kMaxXorLevel; ++level) {
		goals.push_back(xorLevelToGoal(level));
	}
	return goals;
}

std::map<uint8_t, std::string> ChunksHealthCommand::createGoalNames() {
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

void ChunksHealthCommand::printState(const ChunksAvailabilityState& state, bool isPorcelain) const {
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

void ChunksHealthCommand::printState(bool isReplication, const ChunksReplicationState& state,
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

std::string ChunksHealthCommand::print(uint64_t number) const {
	if (number == 0) {
		return "-";
	}
	return std::to_string(number);
}
