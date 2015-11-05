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
#include "master/get_servers_for_new_chunk.h"

#include "common/massert.h"
#include "common/random.h"

void GetServersForNewChunk::prepareData(ChunkCreationHistory &history) {
	// To avoid overflows (weight * chunksCreatedSoFar), we will reset history every million
	// chunks
	const int64_t kMaxChunkCount = 1000000;

	if (history.size() != servers_.size()) {
		history.clear();
	} else {
		for (uint32_t i = 0; i < servers_.size(); ++i) {
			if (servers_[i].server != history[i].server ||
			    servers_[i].weight != history[i].weight ||
			    servers_[i].label != history[i].label ||
			    history[i].chunks_created > kMaxChunkCount) {
				history.clear();
				break;
			}
		}
	}

	// If the list of servers did change or the definition of our goal did change, reset the
	// history
	if (history.empty()) {
		for (const auto &server : servers_) {
			history.emplace_back(server.server, server.label, server.weight,
			                     server.version);
		}
	}

	// Copy history to servers_
	for (uint32_t i = 0; i < servers_.size(); ++i) {
		servers_[i].chunks_created = history[i].chunks_created;
	}

	// Order servers by relative disk usage.
	// random_shuffle to choose randomly if relative disk usage is the same.
	std::random_shuffle(servers_.begin(), servers_.end());
	std::stable_sort(servers_.begin(), servers_.end(),
	                 [](const ChunkserverChunkCounter &a, const ChunkserverChunkCounter &b) {
		                 int64_t aRelativeUsage = a.chunks_created * b.weight;
		                 int64_t bRelativeUsage = b.chunks_created * a.weight;
		                 return (aRelativeUsage < bRelativeUsage ||
		                         (aRelativeUsage == bRelativeUsage && a.weight > b.weight));
		         });
}

std::vector<matocsserventry *> GetServersForNewChunk::chooseServersForLabels(
	ChunkCreationHistory &history, const Goal::Slice::ConstPartProxy &labels, uint32_t min_version,
	std::vector<matocsserventry *> &used) {
	std::vector<matocsserventry *> result;

	// TODO(Haze): It should be optimized also for large number of servers.

	// Choose servers for non-wildcard labels
	for (const auto &label_and_count : labels) {
		int copies_to_be_created = label_and_count.second;
		if (label_and_count.first == MediaLabel::kWildcard) {
			break;
		}
		for (const auto &server : servers_) {
			if (copies_to_be_created == 0) {
				break;
			}
			if (server.version < min_version ||
			    std::find(used.begin(), used.end(), server.server) != used.end()) {
				continue;
			}
			if (server.label == label_and_count.first) {
				result.push_back(server.server);
				used.push_back(server.server);
				--copies_to_be_created;
			}
		}
	}

	int expected_copies = Goal::Slice::countLabels(labels);

	// Add any servers to have the desired number of copies
	for (const auto &server : servers_) {
		if ((int)result.size() >= expected_copies) {
			break;
		}
		if (server.version < min_version ||
		    std::find(used.begin(), used.end(), server.server) != used.end()) {
			continue;
		}
		result.push_back(server.server);
		used.push_back(server.server);
	}

	// Update the history
	for (auto &historyEntry : history) {
		if (std::find(result.begin(), result.end(), historyEntry.server) != result.end()) {
			historyEntry.chunks_created++;
		}
	}

	return result;
}
