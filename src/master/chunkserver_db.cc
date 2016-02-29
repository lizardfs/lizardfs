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

static std::unordered_map<std::pair<uint32_t, uint16_t>, csdbentry, csdb_hash> gCSDB;

int csdb_new_connection(uint32_t ip, uint16_t port, matocsserventry *eptr) {
	auto it = gCSDB.find(std::make_pair(ip, port));
	if (it != gCSDB.end()) {
		if (it->second.eptr != nullptr) {
			return -1;
		}

		it->second.eptr = eptr;
		return 0;
	}

	gCSDB[std::make_pair(ip, port)].eptr = eptr;
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
		gCSDB.erase(it);
		return 1;
	}

	return 0;
}
