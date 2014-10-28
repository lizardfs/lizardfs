#include "common/platform.h"
#include "master/get_servers_for_new_chunk.h"

#include "common/massert.h"
#include "common/random.h"

#include <iostream>
#include <random>
#include <vector>

/**
 * Function deciding if an additional wildcard copy (one of 'wildcardCopies') will be
 * assigned to a group of servers with summaric weight 'weight' (when the total weight is
 * 'totalWeight'), taking into account that the total expected copies number is 'total' and
 * the fact that the considered group of server already have to store 'minimum' non-wildcard
 * copies.
 */
int64_t GetServersForNewChunk::wildcardCopy(int64_t totalWeights, int64_t weight,
		int64_t total, int64_t minimum, int64_t wildcardCopies) {
	int64_t ret = 0;
	if (minimum * totalWeights < total * weight) {
		int64_t aLot = 1000000000;
		int64_t desiredTimesALot = aLot * total * weight / totalWeights;
		if ((desiredTimesALot / aLot - minimum) > 0
				|| int64_t(rndu64_ranged(aLot)) < (desiredTimesALot % aLot)) {
			ret += 1;
		}
	}
	if (ret > wildcardCopies) {
		ret = wildcardCopies;
	}
	return ret;
}

/**
 * Function choosing 'numberOfCopies' from 'servers', using the fact that the total weight
 * of these servers is 'summaricWeight'
 */
std::vector<matocsserventry*> GetServersForNewChunk::chooseNServers(
		std::vector<WeightedServer>& servers, int64_t summaricWeight,
		int64_t numberOfCopies) {
	std::vector<matocsserventry*> result;

	// It is essential that we process the collection starting from the biggest and ending
	// at the lowest weights. This way we ensure that all the servers that should be always
	// chosen (condition 'XX' below) were indeed chosen.
	// Example: consider 3 servers with capacity 2TB, 1TB and 1TB. In that case for all the files
	// in goal=2 one copy should always be placed on the biggest server.
	std::sort(servers.begin(), servers.end(),
			[](WeightedServer s1, WeightedServer s2){ return s1.weight < s2.weight;});
	if (servers.size() > 0)
		sassert(servers[0].weight <= servers[servers.size() - 1].weight);
	auto placed = 0;
	while (placed != numberOfCopies) {
		int64_t serversSize = servers.size();
		sassert(serversSize >= numberOfCopies - placed);
		if (serversSize == numberOfCopies - placed) {
			placed = numberOfCopies;
			for (auto& s : servers) {
				result.push_back(s.server);
			}
			break;
		} else {
			auto s = servers.back();
			servers.pop_back();

			int64_t aLot = 1000000000;
			int64_t magic = (aLot * s.weight * (numberOfCopies - placed)) / summaricWeight;
			if (magic >= aLot/*XX*/ || int64_t(rndu64_ranged(aLot)) < magic) {
				result.push_back(s.server);
				placed++;
			}
			summaricWeight -= s.weight;
		}
	}
	return result;

}

std::vector<matocsserventry*> GetServersForNewChunk::chooseServersForGoal(const Goal& goal) {
	std::vector<matocsserventry*> result;
	auto goalLabels = goal.labels();

	// Compute the number of copies that should be created
	auto copiesNeeded = goal.getExpectedCopies();

	// Cluster servers by labels (using only labels used in goalLabels + kMediaLabelWildcard)
	std::map<MediaLabel, int64_t> sumaricLabelsWeight;
	std::map<MediaLabel, std::vector<WeightedServer>> serversWithLabel;
	int64_t summaricServersWeight = 0;
	for (auto& server : servers_) {
		summaricServersWeight += server.weight;

		auto label = goalLabels[*(server.label)] != 0 ? *(server.label) : kMediaLabelWildcard;
		sumaricLabelsWeight[label] += server.weight;
		serversWithLabel[label].push_back(server);
	}

	// Handle goal that cannot be properly realized, due to a lack of sufficient number of servers
	// with a given label
	for (const auto& labelAndCount : goalLabels) {
		auto& label = labelAndCount.first;
		if (label == kMediaLabelWildcard) {
			continue;
		}
		int64_t count = labelAndCount.second;
		if (int64_t(serversWithLabel[label].size()) < count) {
			goalLabels[kMediaLabelWildcard] += count - serversWithLabel[label].size();
			goalLabels[label] = serversWithLabel[label].size();
		}
	}

	int64_t copiesAssignedSoFar = 0;
	int64_t wildcardsNotYetAssigned = goalLabels[kMediaLabelWildcard];
	int64_t serversStillLeft = servers_.size();

	// For every label decide how many wildcard copies should bo for sure places on servers with
	// this label. The wildcard copies not assigned in this step will be assigned in a random
	// manner (with adequate probabilities), but these ones will be assigned without drawing.
	int64_t wildcardsToBeAssignedWithoutDrawing = 0;
	std::map<MediaLabel, int64_t> wildcardsToBeAssignedWithoutDrawingForLabel;
	for (auto& labelAndCount : sumaricLabelsWeight) {
		auto label = labelAndCount.first;
		int64_t desiredNumberOfCopies = (label != kMediaLabelWildcard ? goalLabels[label] : 0);
		if (summaricServersWeight * desiredNumberOfCopies
				< copiesNeeded * sumaricLabelsWeight[label]) {
			wildcardsToBeAssignedWithoutDrawingForLabel[label] =
					(copiesNeeded * sumaricLabelsWeight[label]
							- summaricServersWeight * desiredNumberOfCopies)
					/ summaricServersWeight;
			wildcardsToBeAssignedWithoutDrawing +=
					wildcardsToBeAssignedWithoutDrawingForLabel[label];
		}
	}
	int64_t wildcardsNotYetAssignedByDrawing = wildcardsNotYetAssigned
			- wildcardsToBeAssignedWithoutDrawing;

	// Copy sumaricLabelsWeight to labelsWeight in any order starting with a wildcard. It is
	// essential that the wildcard is processed first, otherwise in case if we use goal with
	// 2 or more labels and wildcards (like A B _) the wildcard copy would be placed to often
	// on servers labeled with A or B (and to rarely on other servers).
	std::vector<std::pair<MediaLabel, int64_t>> labelsWithCounts;
	labelsWithCounts.emplace_back(kMediaLabelWildcard, sumaricLabelsWeight[kMediaLabelWildcard]);
	std::copy_if(sumaricLabelsWeight.begin(), sumaricLabelsWeight.end(),
			std::back_inserter(labelsWithCounts),
			[](std::pair<MediaLabel, int64_t> s) { return s.first != kMediaLabelWildcard; });
	// For every label decide how many servers with this label will be used, and then choose
	// these servers in a second step
	for (auto& labelAndCount : labelsWithCounts) {
		auto label = labelAndCount.first;

		int64_t desiredNumberOfCopies = (label != kMediaLabelWildcard ? goalLabels[label] : 0);
		// If there are still some wildcard copies to be assigned and the current label isn't
		// already overloaded try to assign it some additional copies
		if (wildcardsNotYetAssigned > 0 &&
				summaricServersWeight * desiredNumberOfCopies <
						(copiesNeeded - copiesAssignedSoFar) * sumaricLabelsWeight[label]) {
			// First assign copies that were previously decided to by assigned to this label
			desiredNumberOfCopies += wildcardsToBeAssignedWithoutDrawingForLabel[label];
			// Second possibly add one server more
			int64_t drawnWildcards = wildcardCopy(
					summaricServersWeight,
					sumaricLabelsWeight[label],
					copiesNeeded - copiesAssignedSoFar,
					desiredNumberOfCopies,
					wildcardsNotYetAssignedByDrawing);
			desiredNumberOfCopies += drawnWildcards;
			wildcardsNotYetAssignedByDrawing -= drawnWildcards;
		}

		// Handle some edge cases.
		// Case when we have not more servers left then copies to be placed somewhere. Use all
		// the servers that are left.
		if (serversStillLeft + copiesAssignedSoFar <= copiesNeeded) {
			desiredNumberOfCopies = serversWithLabel[label].size();
		}
		// Case when we process the last label, in this case we place all the yet unassigned copies
		// on the servers with this label
		if (serversStillLeft == int64_t(serversWithLabel[label].size())) {
			desiredNumberOfCopies = copiesNeeded - copiesAssignedSoFar;
		}
		// Case when we decided to use more servers then we actually have for this label
		auto numberOfCopies = std::min<int64_t>(
				desiredNumberOfCopies, serversWithLabel[label].size());

		// Update auxiliary variables
		if (numberOfCopies > goalLabels[label]) {
			wildcardsNotYetAssigned -= numberOfCopies - goalLabels[label];
		}
		copiesAssignedSoFar += numberOfCopies;
		serversStillLeft -= serversWithLabel[label].size();
		summaricServersWeight -= sumaricLabelsWeight[label];

		// Choose 'numberOfCopies' servers our of all servers with the current label with a
		// proper distribution
		auto chosenServers = chooseNServers(serversWithLabel[label],
				sumaricLabelsWeight[label], numberOfCopies);
		result.insert(result.end(), chosenServers.begin(), chosenServers.end());
	}
	return result;
}
