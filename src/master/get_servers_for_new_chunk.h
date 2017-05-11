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
	ChunkserverChunkCounter()
	    : server(nullptr),
	      label(),
	      weight(),
	      version(),
	      chunks_created(),
	      load_factor() {
	}

	ChunkserverChunkCounter(matocsserventry *server, MediaLabel label, int64_t weight,
	                        uint32_t version, uint8_t load_factor)
	    : server(server),
	      label(std::move(label)),
	      weight(weight),
	      version(version),
	      chunks_created(0),
	      load_factor(load_factor) {
	}

	matocsserventry *server;
	MediaLabel label;
	int64_t weight;
	uint32_t version;

	/// Number of chunks created on this server.
	/// This information would be reset if anything did change (eg. list of servers,
	/// their labels or weights).
	int64_t chunks_created;
	uint8_t load_factor;
};

typedef std::vector<ChunkserverChunkCounter> ChunkCreationHistory;

/*! \brief Class implementing algorithm which chooses servers for a new chunk. */
class GetServersForNewChunk {
public:
	/*! \brief Constructor. */
	GetServersForNewChunk() {
		servers_.reserve(32);
	}

	/*! \brief Adds information about a server.
	 *
	 * \param server pointer to structure describing server.
	 * \param label server's label.
	 * \param weight server priority used in search.
	 * \param version chunk server version.
	 */
	void addServer(matocsserventry *server, const MediaLabel &label, int64_t weight,
	               uint32_t version, uint8_t load_factor) {
		servers_.emplace_back(server, label, weight, version, load_factor);
	}

	/*! \brief Prepare data for subsequent calls to chooseServersForLabels.
	 *
	 * \param history vector with information about previous requests.
	 */
	void prepareData(ChunkCreationHistory &history);

	/*! \brief Chooses servers to fulfill the given goal.
	 *
	 * \param history vector with information about previous requests.
	 * \param labels requested labels for servers.
	 * \param min_version minimum version of chunk server that should be returned.
	 * \param used vector with already used chunk servers.
	 */
	std::vector<matocsserventry *> chooseServersForLabels(ChunkCreationHistory &history,
	                                                      const Goal::Slice::ConstPartProxy &labels,
	                                                      uint32_t min_version,
	                                                      std::vector<matocsserventry *> &used);

protected:
	void sortAvoidingSameIp();

private:
	std::vector<ChunkserverChunkCounter> servers_;
};
