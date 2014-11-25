#include "common/platform.h"
#include "master/get_servers_for_new_chunk.h"

#include "common/massert.h"
#include "common/random.h"

std::vector<matocsserventry*> GetServersForNewChunk::chooseServersForGoal(
		const Goal& goal, ChunkCreationHistory& history) {
	// To avoid overflows (weight * chunksCreatedSoFar), we will reset history every million chunks
	const int64_t kMaxChunkCount = 1000000;

	// Check if the list of servers didn't change
	if (history.servers.size() != servers_.size()) {
		history.servers.clear();
	} else {
		for (uint32_t i = 0; i < servers_.size(); ++i) {
			if (servers_[i].server != history.servers[i].server
					|| servers_[i].weight != history.servers[i].weight
					|| *(servers_[i].label) != history.servers[i].label
					|| history.servers[i].chunksCreatedSoFar > kMaxChunkCount) {
				history.servers.clear();
				break;
			}
		}
	}
	// If the list of servers did change or the definition of our goal did change, reset the history
	if (!(history.goal == goal) || history.servers.empty()) {
		history.goal = goal;
		history.servers.clear();
		for (const auto& server : servers_) {
			history.servers.emplace_back(server.server, *(server.label), server.weight);
		}
	}

	// Copy history to servers_
	for (uint32_t i = 0; i < servers_.size(); ++i) {
		servers_[i].chunkCount = history.servers[i].chunksCreatedSoFar;
	}

	// Order servers by relative disk usage.
	// random_shuffle to choose randomly if relative disk usage is the same.
	std::random_shuffle(servers_.begin(), servers_.end());
	std::stable_sort(servers_.begin(), servers_.end(),
			[](const WeightedServer& a, const WeightedServer& b) {
		int64_t aRelativeUsage = a.chunkCount * b.weight;
		int64_t bRelativeUsage = b.chunkCount * a.weight;
		return (aRelativeUsage < bRelativeUsage
				|| (aRelativeUsage == bRelativeUsage && a.weight > b.weight));
	});

	std::vector<matocsserventry*> result;
	// Choose servers for non-wildcard labels
	for (const auto& labelAndCount : goal.labels()) {
		const MediaLabel& label = labelAndCount.first;
		uint32_t copiesToBeCreated = labelAndCount.second;
		if (label == kMediaLabelWildcard) {
			continue;
		}
		for (const auto& server : servers_) {
			if (copiesToBeCreated == 0) {
				break;
			} else if (*(server.label) == label) {
				result.push_back(server.server);
				copiesToBeCreated--;
			}
		}
	}
	// Add any servers to have the desired number of copies
	for (const auto& server : servers_) {
		if (result.size() == goal.getExpectedCopies()) {
			break;
		}
		if (std::find(result.begin(), result.end(), server.server) == result.end()) {
			result.push_back(server.server);
		}
	}
	// Update the history
	for (auto& historyEntry : history.servers) {
		if (std::find(result.begin(), result.end(), historyEntry.server) != result.end()) {
			historyEntry.chunksCreatedSoFar++;
		}
	}
	return result;
}
