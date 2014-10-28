#pragma once
#include "common/platform.h"

#include <cstdint>
#include <map>
#include <vector>

#include "common/goal.h"

struct matocsserventry;

class GetServersForNewChunk {
public:
	struct WeightedServer {
		WeightedServer(matocsserventry* server, const MediaLabel* label, int64_t weight)
				: server(server),
				  label(label),
				  weight(weight) {
		}

		matocsserventry* server;
		const MediaLabel* label;
		int64_t weight;
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
	std::vector<matocsserventry*> chooseServersForGoal(const Goal& goal);

private:
	int64_t wildcardCopy(int64_t sumOfWeights, int64_t myWeight,
			int64_t total, int64_t minimum, int64_t maximumWildcardCopiesToBeAssigned);

	std::vector<matocsserventry*> chooseNServers(
			std::vector<WeightedServer>& servers,
			int64_t sumaricWeight, int64_t N);

	/// List of servers with labels and weights.
	/// We will choose servers randomly with respect to these weights.
	std::vector<WeightedServer> servers_;
};
