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
	GetServersForNewChunk() : sumOfAllWeights_(0) {
		servers_.reserve(32);
	}

	/// Adds information about a server.
	void addServer(matocsserventry* server, const MediaLabel* label, int64_t weight) {
		servers_.emplace_back(server, label, weight);
		sumOfWeightsForLabel_[*label] += weight;
		sumOfAllWeights_ += weight;
	}

	/// Chooses servers to fulfill the given goal.
	std::vector<matocsserventry*> chooseServersForGoal(const Goal& goal);

private:
	/**
	 * Chooses a random (with respect to weights) server.
	 *
	 * After calling this function, \p chooseServerWithLabel cannot be called anymore.
	 * \returns Server pointer or \p nullptr if no more servers available.
	 */
	matocsserventry* chooseAnyServer();

	/**
	 * Chooses a random (with respect to weights) server with the given label.
	 *
	 * \returns Server pointer or \p nullptr if no more servers available
	 */
	matocsserventry* chooseServerWithLabel(const MediaLabel& label);

	/// Sum of weights of all the servers added by \p addServer.
	int64_t sumOfAllWeights_;

	/// For each label, sum of weights of all the servers with this label added by \p addServer.
	std::map<MediaLabel, int64_t> sumOfWeightsForLabel_;

	/// List of servers with labels and weights.
	/// We will choose servers randomly with respect to these weights.
	std::vector<WeightedServer> servers_;
};
