/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2016
   Skytechnology sp. z o.o..

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"
#include "master/chunkserver_db.h"

#include "common/integer_sequence.h"
#include "common/lizardfs_version.h"
#include "common/media_label.h"
#include "master/matocsserv.h"

#include <cstdint>
#include <unordered_map>

struct csdb_hash {
	std::size_t operator()(const std::pair<uint32_t, uint16_t> &key) const {
		return (std::size_t)key.first ^ ((std::size_t)key.second << 16);
	}
};

/*! \brief This function returns array with list of empty entries.
 *
 * This function fills array in the following way:
 *
 * result[0] = 1 (head of free list)
 * ...
 * result[N-2] = N-1
 * result[N-1] = 0 (null)
 *
 * \param no_name list of indexes (0, 1 ,..., N-1)
 *
 * \return array with list of empty entries
 */
template <std::size_t... Is>
constexpr std::array<csdbentry *, sizeof...(Is)> get_free_element_list(index_sequence<Is...>) {
	return std::array<csdbentry *, sizeof...(Is)>{{
	    reinterpret_cast<csdbentry *>(Is == (sizeof...(Is)-1) ? (std::size_t)0 : Is + 1)...}};
}

static std::unordered_map<std::pair<uint32_t, uint16_t>, csdbentry, csdb_hash> gCSDB;
std::array<csdbentry *, csdbentry::kMaxIdCount> gIdToCSEntry =
	get_free_element_list(make_index_sequence<csdbentry::kMaxIdCount>());

namespace {

std::size_t acquireFreeIndex() {
	// gIdToCSEntry[0] is head of free list
	std::size_t index = reinterpret_cast<std::uintptr_t>(gIdToCSEntry[0]);
	gIdToCSEntry[0] = gIdToCSEntry[index];
	gIdToCSEntry[index] = nullptr;
	return index;
}

void releaseIndex(std::size_t index) {
	gIdToCSEntry[index] = gIdToCSEntry[0];
	gIdToCSEntry[0] = reinterpret_cast<csdbentry*>(index);
}

} // no name

int csdb_new_connection(uint32_t ip, uint16_t port, matocsserventry *eptr) {
	auto it = gCSDB.find(std::make_pair(ip, port));
	if (it != gCSDB.end()) {
		if (it->second.eptr != nullptr) {
			return -1;
		}

		it->second.eptr = eptr;
		return 0;
	}

	int new_id = acquireFreeIndex();
	if (new_id == 0) {
		return -1;
	}

	csdbentry &entry(gCSDB[std::make_pair(ip, port)]);

	entry.csid = new_id;
	entry.eptr = eptr;

	assert(!gIdToCSEntry[new_id]);
	gIdToCSEntry[new_id] = &entry;

	return 1;
}

void csdb_lost_connection(uint32_t ip, uint16_t port) {
	auto it = gCSDB.find(std::make_pair(ip, port));
	if (it != gCSDB.end()) {
		it->second.eptr = nullptr;
	}
}

std::vector<ChunkserverListEntry> csdb_chunkserver_list() {
	std::vector<ChunkserverListEntry> result;
	for (const auto &entry : gCSDB) {
		if (entry.second.eptr != nullptr) {
			ChunkserverListEntry data;
			matocsserv_getserverdata(entry.second.eptr, data);
			result.emplace_back(data);
		} else {
			result.emplace_back(kDisconnectedChunkserverVersion, entry.first.first,
			                    entry.first.second, 0, 0, 0, 0, 0, 0, 0,
			                    MediaLabelManager::kWildcard);
		}
	}
	return result;
}

int csdb_remove_server(uint32_t ip, uint16_t port) {
	auto it = gCSDB.find(std::make_pair(ip, port));

	if (it != gCSDB.end()) {
		if (it->second.eptr != nullptr) {
			return -1;
		}
		int id = it->second.csid;
		gCSDB.erase(it);
		releaseIndex(id);
		return 1;
	}

	return 0;
}

csdbentry *csdb_find(uint32_t ip, uint16_t port) {
	auto it = gCSDB.find(std::make_pair(ip, port));

	if (it != gCSDB.end()) {
		return &(it->second);
	}

	return nullptr;
}
