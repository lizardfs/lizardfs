#include "common/platform.h"
#include "master/get_servers_for_new_chunk.h"

#include <gtest/gtest.h>

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
			getter.addServer(const_cast<matocsserventry*>(serverPtr), &it->second, 1);
		}
		return getter;
	}

	// A function which counts occurrences of labels in the value returned by chooseServersForGoal
	static Goal::Labels countLabels(const std::vector<matocsserventry*>& result) {
		Goal::Labels labelCounts;
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
	{"A1", "A"}, {"A2", "A"}, {"A3", "A"}, {"A4", "A"}, {"A5", "A"},
	{"B1", "B"}, {"B2", "B"}, {"B3", "B"}, {"B4", "B"}, {"B5", "B"},
	{"C1", "C"}, {"C2", "C"}, {"C3", "C"}, {"C4", "C"}, {"C5", "C"},
};

constexpr int kTestAccuracy = 100;

TEST_F(GetServersForNewChunkTests, ChooseServers1) {
	// servers: A A B B C
	//    goal: A _
	Goal goal("goal", {{"A", 1}, {kMediaLabelWildcard, 1}});
	for (int i = 0; i < kTestAccuracy; ++i) {
		auto getter = createGetServersForNewChunk({"A1", "A2", "B1", "B2", "C1"});
		auto result = getter.chooseServersForGoal(goal);
		ASSERT_EQ(2U, result.size());

		auto labelCounts = countLabels(result);
		ASSERT_GE(labelCounts["A"], 1);
	}
}

TEST_F(GetServersForNewChunkTests, ChooseServers2) {
	// servers: A A B B C
	//    goal: A _ _
	Goal goal("goal", {{"A", 1}, {kMediaLabelWildcard, 2}});
	for (int i = 0; i < kTestAccuracy; ++i) {
		auto getter = createGetServersForNewChunk({"A1", "A2", "B1", "B2", "C1"});
		auto result = getter.chooseServersForGoal(goal);
		ASSERT_EQ(3U, result.size());

		auto labelCounts = countLabels(result);
		ASSERT_GE(labelCounts["A"], 1);
	}
}

TEST_F(GetServersForNewChunkTests, ChooseServers3) {
	// servers: A A B B C
	//    goal: A C
	Goal goal("goal", {{"A", 1}, {"C", 1}});
	for (int i = 0; i < kTestAccuracy; ++i) {
		auto getter = createGetServersForNewChunk({"A1", "A2", "B1", "B2", "C1"});
		auto result = getter.chooseServersForGoal(goal);
		ASSERT_EQ(2U, result.size());

		auto labelCounts = countLabels(result);
		ASSERT_EQ(labelCounts["A"], 1);
		ASSERT_EQ(labelCounts["C"], 1);
	}
}

TEST_F(GetServersForNewChunkTests, ChooseServers4) {
	// servers: A B B B C
	//    goal: A _ C
	Goal goal("goal", {{"A", 1}, {kMediaLabelWildcard, 1}, {"C", 1}});
	for (int i = 0; i < kTestAccuracy; ++i) {
		auto getter = createGetServersForNewChunk({"A1", "B1", "B2", "B3", "C1"});
		auto result = getter.chooseServersForGoal(goal);
		ASSERT_EQ(3U, result.size());

		auto labelCounts = countLabels(result);
		ASSERT_EQ(labelCounts["A"], 1);
		ASSERT_EQ(labelCounts["B"], 1);
		ASSERT_EQ(labelCounts["C"], 1);
	}
}

TEST_F(GetServersForNewChunkTests, ChooseServers5) {
	// servers: A B B B C
	//    goal: A A A
	Goal goal("goal", {{"A", 3}});
	for (int i = 0; i < kTestAccuracy; ++i) {
		auto getter = createGetServersForNewChunk({"A1", "B1", "B2", "B3", "C1"});
		auto result = getter.chooseServersForGoal(goal);
		ASSERT_EQ(3U, result.size());

		auto labelCounts = countLabels(result);
		ASSERT_EQ(labelCounts["A"], 1);
	}
}

