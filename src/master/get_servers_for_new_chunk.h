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

#pragma once
#include "common/platform.h"

#include <cstdint>
#include <map>
#include <vector>

#include "common/goal.h"

struct matocsserventry;

/// A struct used to remember how many chunks were created on a given server.
/// We remember the server's data (pointer, label, weight) to be able to verify if this
/// information didn't change.
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

	/// Number of chunks created on this sever.
	/// This information would be reset if anything did change (eg. list of servers,
	/// their labels or weights).
	int64_t chunksCreatedSoFar;
};

/// History of chunk creation for some goal
struct ChunkCreationHistory {
	ChunkCreationHistory() {}
	ChunkCreationHistory(Goal goal) : goal(std::move(goal)) {}

	/// The goal.
	Goal goal;

	/// Information about chunks created on each server for the \p goal.
	std::vector<ChunkserverChunkCounter> servers;
};

/// An algorithm which chooses servers for a new chunk.
class GetServersForNewChunk {
public:
	struct WeightedServer {
		WeightedServer(matocsserventry* server, const MediaLabel label, int64_t weight)
				: server(server),
				  label(label),
				  weight(weight),
				  chunkCount(0) {
		}

		matocsserventry* server;
		MediaLabel label;
		int64_t weight;

		// Used internally by chooseServersForGoal algorithm
		int64_t chunkCount;
	};

	/// A constructor.
	GetServersForNewChunk() {
		servers_.reserve(32);
	}

	/// Adds information about a server.
	void addServer(matocsserventry* server, const MediaLabel& label, int64_t weight) {
		servers_.emplace_back(server, label, weight);
	}

	/// Chooses servers to fulfill the given goal.
	std::vector<matocsserventry*> chooseServersForGoal(
			const Goal& goal, ChunkCreationHistory& history);

private:
	std::vector<WeightedServer> servers_;
};
