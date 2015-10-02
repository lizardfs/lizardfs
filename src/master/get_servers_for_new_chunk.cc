/*
   Copyright 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o.

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
#include "master/chunks.h"
#include "master/matocsserv.h"

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
			                     server.version, server.load_factor);
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
					 return std::make_tuple(aRelativeUsage, -a.weight, a.load_factor)
					 < std::make_tuple(bRelativeUsage, -b.weight, b.load_factor);
			 });

	if (gAvoidSameIpChunkservers) {
		sortAvoidingSameIp();
	}
}

void GetServersForNewChunk::sortAvoidingSameIp() {
	IpCounter ip_counter;
	small_vector<int, 16> occurrence_offset(1);
	std::vector<int> serv_occurrence_no(servers_.size(), 0);

	int tmp_index;
	uint32_t tmp_ip;
	for (size_t i = 0; i < servers_.size(); ++i) {
		tmp_ip = matocsserv_get_servip(servers_[i].server);
		tmp_index = ip_counter[tmp_ip]++;
		serv_occurrence_no[i] = tmp_index;
		assert(tmp_index < (int)occurrence_offset.size());
		if (tmp_index + 1 >= (int)occurrence_offset.size()) {
			occurrence_offset.push_back(1);
		} else {
			++occurrence_offset[tmp_index + 1];
		}
	}
	for (size_t i = 1; i < occurrence_offset.size(); ++i) {
		occurrence_offset[i] += occurrence_offset[i - 1];
	}
	std::vector<ChunkserverChunkCounter> new_order(servers_.size());
	for (size_t i = 0; i < servers_.size(); ++i) {
		tmp_index = occurrence_offset[serv_occurrence_no[i]]++;
		new_order[tmp_index] = servers_[i];
	}
	servers_ = std::move(new_order);
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
