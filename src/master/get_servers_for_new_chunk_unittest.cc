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
#include "common/media_label.h"
#include "master/get_servers_for_new_chunk.h"

#include <gtest/gtest.h>

#include "master/goal_config_loader.h"

Goal::Slice::ConstPartProxy createProxy(Goal::Slice::Labels &label) {
	return Goal::Slice::ConstPartProxy(
	    vector_range<const Goal::Slice::DataContainer, Goal::Slice::SizeContainer::value_type>(
	        label.data(), 0, label.size()));
}

// FIXME
// A base class for all tests
class GetServersForNewChunkTests : public ::testing::Test {
protected:
	// A map 'server_name' -> 'server_label' used in these tests
	typedef std::map<std::string, MediaLabel> AllServers;

	// A function which returns a GetServersForNewChunk object for the given (by name) servers
	static GetServersForNewChunk createGetServersForNewChunk(std::vector<std::string> servers) {
		GetServersForNewChunk getter;
		for (const std::string& server : servers) {
			auto it = allServers.find(server);
			if (it == allServers.end()) {
				throw std::runtime_error("Unknown server " + server);
			}
			// We will use the address of the entry in our map as a matocsserventry* pointer.
			const matocsserventry* serverPtr = reinterpret_cast<const matocsserventry*>(&(*it));
			getter.addServer(const_cast<matocsserventry*>(serverPtr), it->second, 1, 0, 0);
		}
		return getter;
	}

	// A function which counts occurrences of labels in the value returned by chooseServersForGoal
	static Goal::Slice::Labels countLabels(const std::vector<matocsserventry*>& result) {
		Goal::Slice::Labels labelCounts;
		for (matocsserventry* server : result) {
			const AllServers::value_type& serverEntry =
					*reinterpret_cast<const AllServers::value_type*>(server);
			labelCounts[serverEntry.second]++;
		}
		return labelCounts;
	}

	// A map 'server_name' -> 'server_label' with all the servers used in these tests
	static const std::map<std::string, MediaLabel> allServers;
};

// Initial values for the map: three labels, file server for each
const std::map<std::string, MediaLabel> GetServersForNewChunkTests::allServers{
	{"A1", MediaLabel("A")},
	{"A2", MediaLabel("A")},
	{"A3", MediaLabel("A")},
	{"A4", MediaLabel("A")},
	{"A5", MediaLabel("A")},
	{"B1", MediaLabel("B")},
	{"B2", MediaLabel("B")},
	{"B3", MediaLabel("B")},
	{"B4", MediaLabel("B")},
	{"B5", MediaLabel("B")},
	{"C1", MediaLabel("C")},
	{"C2", MediaLabel("C")},
	{"C3", MediaLabel("C")},
	{"C4", MediaLabel("C")},
	{"C5", MediaLabel("C")},
};

constexpr int kTestAccuracy = 100;

TEST_F(GetServersForNewChunkTests, ChooseServers0) {
	// servers: _ _ _
	//    goal: _ _ _ _
	ChunkCreationHistory history;
	Goal::Slice::Labels labels = {{MediaLabel::kWildcard, 4}};
	for (int i = 0; i < kTestAccuracy; ++i) {
		std::vector<matocsserventry *> used;
		auto getter = createGetServersForNewChunk({"A1", "A2", "A3"});
		getter.prepareData(history);
		auto result = getter.chooseServersForLabels(history, createProxy(labels), 0, used);
		ASSERT_EQ(3U, result.size());

		auto labelCounts = countLabels(result);
		ASSERT_GE(labelCounts[MediaLabel("A")], 3);
	}
}

TEST_F(GetServersForNewChunkTests, ChooseServers1) {
	// servers: A A B B C
	//    goal: A _
	ChunkCreationHistory history;
	Goal::Slice::Labels labels = {{MediaLabel::kWildcard, 1}, {MediaLabel("A"), 1}};
	for (int i = 0; i < kTestAccuracy; ++i) {
		std::vector<matocsserventry *> used;
		auto getter = createGetServersForNewChunk({"A1", "A2", "B1", "B2", "C1"});
		getter.prepareData(history);
		auto result = getter.chooseServersForLabels(history, createProxy(labels), 0, used);
		ASSERT_EQ(2U, result.size());

		auto labelCounts = countLabels(result);
		ASSERT_GE(labelCounts[MediaLabel("A")], 1);
	}
}

TEST_F(GetServersForNewChunkTests, ChooseServers2) {
	// servers: A A B B C
	//    goal: A _ _
	ChunkCreationHistory history;
	Goal::Slice::Labels labels = {{MediaLabel::kWildcard, 1}, {MediaLabel("A"), 2}};
	for (int i = 0; i < kTestAccuracy; ++i) {
		std::vector<matocsserventry *> used;
		auto getter = createGetServersForNewChunk({"A1", "A2", "B1", "B2", "C1"});
		getter.prepareData(history);
		auto result = getter.chooseServersForLabels(history, createProxy(labels), 0, used);
		ASSERT_EQ(3U, result.size());

		auto labelCounts = countLabels(result);
		ASSERT_GE(labelCounts[MediaLabel("A")], 1);
	}
}

TEST_F(GetServersForNewChunkTests, ChooseServers3) {
	// servers: A A B B C
	//    goal: A C
	ChunkCreationHistory history;
	Goal::Slice::Labels labels = {{MediaLabel("A"), 1}, {MediaLabel("C"), 1}};
	for (int i = 0; i < kTestAccuracy; ++i) {
		std::vector<matocsserventry *> used;
		auto getter = createGetServersForNewChunk({"A1", "A2", "B1", "B2", "C1"});
		getter.prepareData(history);
		auto result = getter.chooseServersForLabels(history, createProxy(labels), 0, used);
		ASSERT_EQ(2U, result.size());

		auto labelCounts = countLabels(result);
		ASSERT_EQ(labelCounts[MediaLabel("A")], 1);
		ASSERT_EQ(labelCounts[MediaLabel("C")], 1);
	}
}

TEST_F(GetServersForNewChunkTests, ChooseServers4) {
	// servers: A B B B C
	//    goal: A _ C
	ChunkCreationHistory history;
	Goal::Slice::Labels labels = {{MediaLabel("A"), 1}, {MediaLabel::kWildcard, 1},  {MediaLabel("C"), 1}};
	for (int i = 0; i < kTestAccuracy; ++i) {
		std::vector<matocsserventry *> used;
		auto getter = createGetServersForNewChunk({"A1", "B1", "B2", "B3", "C1"});
		getter.prepareData(history);
		auto result = getter.chooseServersForLabels(history, createProxy(labels), 0, used);
		ASSERT_EQ(3U, result.size());

		auto labelCounts = countLabels(result);
		ASSERT_EQ(labelCounts[MediaLabel("A")], 1);
		ASSERT_EQ(labelCounts[MediaLabel("B")], 1);
		ASSERT_EQ(labelCounts[MediaLabel("C")], 1);
	}
}

TEST_F(GetServersForNewChunkTests, ChooseServers5) {
	// servers: A B B B C
	//    goal: A A A
	ChunkCreationHistory history;
	Goal::Slice::Labels labels = {{MediaLabel("A"), 3}};
	for (int i = 0; i < kTestAccuracy; ++i) {
		std::vector<matocsserventry *> used;
		auto getter = createGetServersForNewChunk({"A1", "B1", "B2", "B3", "C1"});
		getter.prepareData(history);
		auto result = getter.chooseServersForLabels(history, createProxy(labels), 0, used);
		ASSERT_EQ(3U, result.size());

		auto labelCounts = countLabels(result);
		ASSERT_EQ(labelCounts[MediaLabel("A")], 1);
	}
}

void testScenario(double maxUsageDifference, std::vector<std::string> goalDefs,
		std::vector<std::pair<std::string, int>> servers) {
	ChunkCreationHistory history;
	SCOPED_TRACE(std::string("Testing scenario:\n") +
			"   Goal: " + std::accumulate(goalDefs.begin(), goalDefs.end(), std::string()) + "\n" +
			"Servers: " + ::testing::PrintToString(servers) + "\n"
	);

	Goal goal;
	for (auto goalDef : goalDefs) {
		goal.mergeIn(goal_config::parseLine("1 goal: " + goalDef).second);
	}

	// Do many simulations and count chunks which will be placed on each server
	constexpr int kDistributionTestAccuracy = 10000;
	std::vector<int> chunksOnServer(servers.size(), 0);
	for (int i = 0; i < kDistributionTestAccuracy; ++i) {
		GetServersForNewChunk algorithm;
		for (int server = 0; server < int(servers.size()); ++server) {
			algorithm.addServer(reinterpret_cast<matocsserventry*>(server + 1),
					MediaLabel(servers[server].first), servers[server].second, 0, 0);
		}
		algorithm.prepareData(history);

		std::vector<matocsserventry *> result, slice_result;
		std::vector<matocsserventry *> partial, used;
		for (const auto &slice : goal) {
			for (int i = 0; i < slice.size(); ++i) {
				partial = algorithm.chooseServersForLabels(
				        history, slice[i],
				        0,
				        used);
				for (const auto &server : partial) {
					slice_result.push_back(server);
				}
			}

			result.insert(result.end(), slice_result.begin(), slice_result.end());
		}

		for (matocsserventry* ptr : result) {
			++chunksOnServer[reinterpret_cast<intptr_t>(ptr) - 1];
		}
	}

	// Calculate disk usage on each server
	int sumOfWeights = 0;
	double chunksPerWeightUnit = 0;
	int sumOfChunks = 0;
	for (int server = 0; server < int(servers.size()); ++server) {
		sumOfWeights += servers[server].second;
		sumOfChunks += chunksOnServer[server];
	}
	chunksPerWeightUnit = sumOfChunks * 1.0 / sumOfWeights;
	double minUsage = 1.0;
	double maxUsage = 0.0;
	std::vector<double> diskUsageOnServer(servers.size(), 0.0);
	for (int server = 0; server < int(servers.size()); ++server) {
		double usage = chunksOnServer[server] / (servers[server].second * chunksPerWeightUnit);
		diskUsageOnServer[server] = usage;
		minUsage = std::min(usage, minUsage);
		maxUsage = std::max(usage, maxUsage);
	}
	EXPECT_LE(maxUsage - minUsage, maxUsageDifference)
			<< "Disk usage on servers: " << ::testing::PrintToString(diskUsageOnServer);
}

TEST_F(GetServersForNewChunkTests, ChunkDistribution) {
	double acceptableDifference = 0.01;

	testScenario(0.0, {"_"}, {
			{"_", 10}, {"_", 10}, {"_", 10}, {"_", 10}, {"_", 10},
	});

	testScenario(0.0, {"A _"}, {
			{"_", 20}, {"_", 10}, {"_", 10},
	});

	testScenario(acceptableDifference, {"_"}, {
			{"_", 10}, {"_", 20}, {"_", 30}, {"_", 40},
	});

	testScenario(acceptableDifference, {"_"}, {
			{"_", 10}, {"_", 12}, {"_", 20}, {"_", 25}, {"_", 32}, {"_", 41}, {"_", 149},
	});

	testScenario(0.0, {"_ _"}, {
			{"_", 10}, {"_", 10}, {"_", 10}, {"_", 10}, {"_", 10},
	});

	testScenario(acceptableDifference, {"_ _"}, {
			{"_", 10}, {"_", 20}, {"_", 30}, {"_", 40},
	});

	testScenario(acceptableDifference, {"_ _"}, {
			{"_", 10}, {"_", 12}, {"_", 20}, {"_", 25}, {"_", 32}, {"_", 41}, {"_", 49},
	});

	testScenario(acceptableDifference, {"_ _"}, {
			{"_", 10}, {"_", 10}, {"_", 40}, {"_", 10}, {"_", 10},
	});

	testScenario(acceptableDifference, {"_ _ _"}, {
			{"_", 10}, {"_", 12}, {"_", 20}, {"_", 25}, {"_", 32}, {"_", 41}, {"_", 49},
	});

	testScenario(acceptableDifference, {"A _"}, {
			{"A", 10}, {"A", 10}, {"A", 10}, {"A", 10}, {"A", 10},
	});

	testScenario(acceptableDifference, {"A _"}, {
			{"A", 10}, {"A", 10},
			{"B", 10}, {"B", 10},
	});

	testScenario(acceptableDifference, {"A _"}, {
			{"A", 10}, {"A", 10}, {"A", 10},
			{"B", 10}, {"B", 10},
	});

	testScenario(acceptableDifference, {"A _"}, {
			{"A", 10}, {"A", 10}, {"A", 10}, {"A", 10},
			{"B", 10}, {"B", 10},
	});

	testScenario(acceptableDifference, {"A _"}, {
			{"A", 10}, {"A", 10}, {"A", 10}, {"A", 10},
			{"B", 10}, {"B", 10}, {"B", 10},
	});

	testScenario(acceptableDifference, {"A _"}, {
			{"A", 10}, {"A", 15}, {"A", 15}, {"A", 20},
			{"B", 10}, {"B", 15}, {"B", 25},
	});

	testScenario(acceptableDifference, {"A _"}, {
			{"A", 10}, {"A", 10}, {"A", 10}, {"A", 10},
			{"B", 10}, {"B", 10},
			{"C", 10}, {"C", 10},
	});

	testScenario(acceptableDifference, {"A _"}, {
			{"A", 10}, {"A", 10}, {"A", 10}, {"A", 10},
			{"B", 10}, {"B", 10},
			{"C", 10},
	});

	testScenario(acceptableDifference, {"A A _"}, {
			{"A", 10}, {"A", 10}, {"A", 10}, {"A", 10}, {"A", 10},
			{"B", 10}, {"B", 10},
	});

	testScenario(acceptableDifference, {"A A _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30}, {"A", 30},
			{"B", 20}, {"B", 25},
	});

	testScenario(acceptableDifference, {"A A _ _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30}, {"A", 35},
			{"B", 20}, {"B", 22}, {"B", 25}, {"B", 29},
	});

	testScenario(acceptableDifference, {"_ _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30}, {"A", 35},
			{"B", 20}, {"B", 22}, {"B", 25}, {"B", 29},
	});

	testScenario(acceptableDifference, {"_ _ _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30}, {"A", 35},
			{"B", 20}, {"B", 22}, {"B", 25}, {"B", 29},
	});

	testScenario(acceptableDifference, {"_ _ _ _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30}, {"A", 35},
			{"B", 20}, {"B", 22}, {"B", 25}, {"B", 29},
	});

	testScenario(acceptableDifference, {"A B _"}, {
			{"A", 10}, {"A", 10}, {"A", 10}, {"A", 10},
			{"B", 10}, {"B", 10}, {"B", 10},
	});

	testScenario(acceptableDifference, {"A B _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30},
			{"B", 10}, {"B", 20}, {"B", 20},
	});

	testScenario(acceptableDifference, {"A A A B B C _ _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30}, {"A", 20}, {"A", 30},
			{"B", 30}, {"B", 20}, {"B", 20}, {"B", 30},
			{"C", 30}, {"C", 30},
			{"_", 10}, {"_", 20}, {"_", 20},
	});

	testScenario(acceptableDifference, {"A B _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30},
			{"B", 10}, {"B", 20}, {"B", 15}, {"B", 25},
			{"C", 10}, {"C", 10},
	});

	testScenario(acceptableDifference, {"A A B B _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30}, {"A", 40},
			{"B", 10}, {"B", 20}, {"B", 15}, {"B", 25}, {"B", 25},
			{"C", 10}, {"C", 10},
	});

	testScenario(acceptableDifference, {"A _ _ _ _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30}, {"A", 40},
			{"B", 10}, {"B", 20}, {"B", 15}, {"B", 25}, {"B", 25},
			{"C", 10}, {"C", 10},
	});

	testScenario(acceptableDifference, {"A _ _ _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30}, {"A", 40},
			{"B", 10}, {"B", 20}, {"B", 15}, {"B", 25}, {"B", 25},
			{"C", 10}, {"C", 10},
	});

	testScenario(acceptableDifference, {"A B C _ _ _ _ _"}, {
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30}, {"A", 40},
			{"A", 10}, {"A", 20}, {"A", 20}, {"A", 30}, {"A", 40},
			{"B", 10}, {"B", 20}, {"B", 15}, {"B", 25}, {"B", 25},
			{"B", 10}, {"B", 20}, {"B", 15}, {"B", 25}, {"B", 25},
			{"C", 20}, {"C", 30}, {"C", 40},
			{"C", 20}, {"C", 30}, {"C", 40},
	});

	testScenario(acceptableDifference, {"A A A B B C D E E E E F G H _ _ _ _ _ _"}, {
			{"A", 1}, {"A", 1}, {"A", 1}, {"A", 1},
			{"B", 1}, {"B", 1}, {"B", 1},
			{"C", 1},
			{"D", 1},
			{"E", 1}, {"E", 1}, {"E", 1}, {"E", 1}, {"E", 1},
			{"F", 1},
			{"G", 1}, {"G", 1}, {"G", 1}, {"G", 1},
			{"H", 1},
	});

	// Xor scenarios
	testScenario(0.0, {"$xor3{_ _ _ _}"}, {
			{"A", 1}, {"A", 1}, {"A", 1}, {"A", 1},
	});

	testScenario(acceptableDifference, {"$xor2{A B _}", "$xor2{B C _}"}, {
			{"A", 1}, {"B", 1}, {"C", 1}
	});

	testScenario(acceptableDifference, {"$xor2{A A B}", "C _ _ _"}, {
			{"A", 1}, {"A", 1}, {"B", 1}, {"C", 1}
	});

	testScenario(acceptableDifference, {"$xor2{_ _ _}", "A B"}, {
			{"A", 2}, {"B", 2}, {"C", 1}, {"C", 1}, {"C", 1}
	});

	testScenario(0.5, {"$xor2{A B C}", "$xor4{A A B B C}", "$xor5{A A A B B B}"}, {
			{"A", 1}, {"A", 1}, {"A", 1}, {"B", 1}, {"B", 1}, {"B", 1}, {"C", 1}, {"C", 1}, {"C", 1}
	});

	testScenario(0.0, {"$xor3{A B C _}", "C B _ _"}, {
			{"A", 2}, {"B", 2}, {"B", 2}, {"C", 2}
	});
}
