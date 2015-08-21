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
#include "admin/chunk_health_command.h"

#include <iostream>

#include "protocol/cltoma.h"
#include "protocol/matocl.h"
#include "common/server_connection.h"

std::vector<uint8_t> ChunksHealthCommand::goals;
std::map<uint8_t, std::string> ChunksHealthCommand::goalNames;

const std::string ChunksHealthCommand::kOptionAvailability = "--availability";
const std::string ChunksHealthCommand::kOptionReplication = "--replication";
const std::string ChunksHealthCommand::kOptionDeletion = "--deletion";

std::string ChunksHealthCommand::name() const {
	return "chunks-health";
}

LizardFsProbeCommand::SupportedOptions ChunksHealthCommand::supportedOptions() const {
	return {
		{kPorcelainMode,      kPorcelainModeDescription},
		{kOptionAvailability, "Print report about availability of chunks."},
		{kOptionReplication,  "Print report about about number of chunks that need replication."},
		{kOptionDeletion,     "Print report about about number of chunks that need deletion."},
	};
}

void ChunksHealthCommand::usage() const {
	std::cerr << name()  << " <master ip> <master port>\n";
	std::cerr << "    Returns chunks health reports in the installation.\n";
	std::cerr << "    By default (if no report is specified) all reports will be shown.\n";
	std::cerr << "    In replication and deletion states, the column means the number of chunks\n";
	std::cerr << "    with number of copies specified in the label to replicate/delete.\n";
}

void ChunksHealthCommand::initializeGoals(ServerConnection& connection) {
	if (!goals.empty()) {
		return;
	}

	std::vector<SerializedGoal> serializedGoals;
	auto request = cltoma::listGoals::build(true);
	auto response = connection.sendAndReceive(request, LIZ_MATOCL_LIST_GOALS);
	matocl::listGoals::deserialize(response, serializedGoals);

	for (const SerializedGoal& goal : serializedGoals) {
		goals.push_back(goal.id);
		goalNames[goal.id] = goal.name;
	}
}

void ChunksHealthCommand::run(const Options& options) const {
	if (options.arguments().size() != 2) {
		throw WrongUsageException("Expected <master ip> and <master port> for " + name() + '\n');
	}

	ServerConnection connection(options.argument(0), options.argument(1));
	bool regularOnly = false;
	auto request = cltoma::chunksHealth::build(regularOnly);
	auto response = connection.sendAndReceive(request, LIZ_MATOCL_CHUNKS_HEALTH);
	ChunksAvailabilityState availability;
	ChunksReplicationState replication;
	matocl::chunksHealth::deserialize(response, regularOnly, availability, replication);
	if (regularOnly) {
		throw Exception("Incorrect response type received");
	}

	initializeGoals(connection);

	bool showAllReports = !options.isSet(kOptionAvailability)
			&& !options.isSet(kOptionReplication)
			&& !options.isSet(kOptionDeletion);
	if (showAllReports || options.isSet(kOptionAvailability)) {
		printState(availability, options.isSet(kPorcelainMode));
	}
	if (showAllReports || options.isSet(kOptionReplication)) {
		printState(true, replication, options.isSet(kPorcelainMode));
	}
	if (showAllReports || options.isSet(kOptionDeletion)) {
		printState(false, replication, options.isSet(kPorcelainMode));
	}

}

void ChunksHealthCommand::printState(const ChunksAvailabilityState& state, bool isPorcelain) const {
	if (isPorcelain) {
		for (uint8_t goal : goals) {
			std::cout << "AVA"
					<< ' ' << goalNames.at(goal)
					<< ' ' << state.safeChunks(goal)
					<< ' ' << state.endangeredChunks(goal)
					<< ' ' << state.lostChunks(goal) << std::endl;
		}
	} else {
		std::cout << "Chunks availability state:" << std::endl;
		std::cout << "\tGoal\tSafe\tUnsafe\tLost" << std::endl;
		for (uint8_t goal : goals) {
			if (state.safeChunks(goal) + state.endangeredChunks(goal)
					+ state.lostChunks(goal) == 0) {
				continue;
			}
			std::cout << '\t' << goalNames.at(goal)
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
		for (uint8_t goal : goals) {
			std::cout << (isReplication ? "REP" : "DEL") << ' ' << goalNames.at(goal);
			for (uint32_t part = 0; part < ChunksReplicationState::kMaxPartsCount; ++part) {
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
		for (uint32_t i = 0; i < ChunksReplicationState::kMaxPartsCount; ++i) {
			std::cout << '\t' << i;
		}
		std::cout << '+' << std::endl;

		for (uint8_t goal : goals) {
			std::string line = '\t' + goalNames.at(goal);
			uint64_t sum = 0;
			for (uint32_t part = 0; part < ChunksReplicationState::kMaxPartsCount; ++part) {
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
