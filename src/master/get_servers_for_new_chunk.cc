#include "common/platform.h"
#include "master/get_servers_for_new_chunk.h"

#include "common/massert.h"
#include "common/random.h"

std::vector<matocsserventry*> GetServersForNewChunk::chooseServersForGoal(const Goal& goal) {
	std::vector<matocsserventry*> result;

	// Choose servers for non-wildcard labels
	for (const auto& labelAndCount : goal.labels()) {
		const auto& currentLabel = labelAndCount.first;
		if (currentLabel == kMediaLabelWildcard) {
			continue;
		}
		int32_t serversChosenForLabel = 0;
		// Choose one server as long as we need more servers and there are
		while (serversChosenForLabel < labelAndCount.second) {
			matocsserventry* server = chooseServerWithLabel(currentLabel);
			if (server == nullptr) {
				// No more servers for this label
				break;
			}
			result.push_back(server);
			serversChosenForLabel++;
		}
	}

	// Choose any servers to get desired number of copies
	while (result.size() < goal.getExpectedCopies() && sumOfAllWeights_ > 0) {
		matocsserventry* server = chooseAnyServer();
		if (server == nullptr) {
			// No more servers
			break;
		}
		result.push_back(server);
	}

	return result;
}

matocsserventry* GetServersForNewChunk::chooseAnyServer() {
	if (sumOfAllWeights_ == 0) {
		// There are no more servers
		return nullptr;
	}
	int64_t weightsToSkip = rndu64_ranged(sumOfAllWeights_);
	for (auto& server : servers_) {
		weightsToSkip -= server.weight;
		if (weightsToSkip < 0) {
			sumOfAllWeights_ -= server.weight;
			server.weight = 0;
			return server.server;
		}
	}
	return nullptr; // Should never happen
}

matocsserventry* GetServersForNewChunk::chooseServerWithLabel(const MediaLabel& label) {
	sassert(label != kMediaLabelWildcard);
	int64_t& sumOfWeightsForCurrentLabel = sumOfWeightsForLabel_[label];
	if (sumOfWeightsForCurrentLabel == 0) {
		// There are no more servers for this label
		return nullptr;
	}
	int64_t weightsToSkip = rndu64_ranged(sumOfWeightsForCurrentLabel);
	for (auto& server : servers_) {
		if (*(server.label) != label) {
			continue; // ignore servers with non-matching labels
		}
		weightsToSkip -= server.weight;
		if (weightsToSkip < 0) {
			// change weight of this server to 0, so we won't choose it more than one
			sumOfWeightsForCurrentLabel -= server.weight;
			sumOfAllWeights_ -= server.weight;
			server.weight = 0;
			return server.server;
		}
	}
	return nullptr; // Should never happen
}
