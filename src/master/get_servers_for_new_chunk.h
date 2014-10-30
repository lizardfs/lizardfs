#pragma once
#include "common/platform.h"

#include <cstdint>
#include <map>
#include <vector>

#include "common/goal.h"

struct matocsserventry;

struct ChunkserverChunkCounter {
	ChunkserverChunkCounter(matocsserventry* server, MediaLabel label, int64_t weight)
			: server(server),
			  label(std::move(label)),
			  weight(weight),
			  chunksCreatedSoFar(0) {
	}

	matocsserventry* server;
	MediaLabel label;
	int64_t weight;
	int64_t chunksCreatedSoFar;
};

struct ChunkCreationHistory {
	ChunkCreationHistory() {}
	ChunkCreationHistory(Goal goal) : goal(std::move(goal)) {}

	Goal goal;
	std::vector<ChunkserverChunkCounter> servers;
};

class GetServersForNewChunk {
public:
	struct WeightedServer {
		WeightedServer(matocsserventry* server, const MediaLabel* label, int64_t weight)
				: server(server),
				  label(label),
				  weight(weight),
				  chunkCount(0) {
		}

		matocsserventry* server;
		const MediaLabel* label;
		int64_t weight;

		// Used internally by chooseServersForGoal algorithm
		int64_t chunkCount;
	};

	/// A constructor.
	GetServersForNewChunk() {
		servers_.reserve(32);
	}

	/// Adds information about a server.
	void addServer(matocsserventry* server, const MediaLabel* label, int64_t weight) {
		servers_.emplace_back(server, label, weight);
	}

	/// Chooses servers to fulfill the given goal.
	std::vector<matocsserventry*> chooseServersForGoal(
			const Goal& goal, ChunkCreationHistory& history);

private:
	std::vector<WeightedServer> servers_;
};
