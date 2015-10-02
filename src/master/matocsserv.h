/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

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

#pragma once

#include "common/platform.h"

#include <inttypes.h>
#include <vector>

#include "common/chunk_part_type.h"
#include "common/goal.h"
#include "common/media_label.h"
#include "master/get_servers_for_new_chunk.h"
#include "protocol/chunkserver_list_entry.h"

/// A struct representing a chunkserver.
struct matocsserventry;

struct csdbentry;

/// A list of chunkservers.
typedef std::vector<matocsserventry*> Chunkservers;

/// A struct used in matocsserv_getservers_sorted
struct ServerWithUsage {
	ServerWithUsage() : server(nullptr), disk_usage(), label() {
	}

	ServerWithUsage(matocsserventry* server, double disk_usage, const MediaLabel& label)
			: server(server),
			  disk_usage(disk_usage),
			  label(label) {
	}

	matocsserventry* server;
	double disk_usage;
	MediaLabel label;
};

typedef flat_map<uint32_t, int, small_vector<std::pair<uint32_t, int>, 16> > IpCounter;

/*! \brief Get list of chunkservers for replication with the given label.
 *
 * This function returns a list of chunkservers that currently don't exceed the given limit of
 * chunks replicated into them. Servers with 99% disk usage are treated as non-existing, thus not
 * returned. The returned servers are randomly shuffled, but if the \p label is not a
 * \p MediaLabel::kWildcard, then servers with this label would be placed in front of the returned
 * list and \p returnedMatching would be set to the number of them.
 *
 * \param label - the requested label.
 * \param min_chunkserver_version - return only chunkservers with higher (or equal) version.
 * \param replicationWriteLimit - return only chunkservers with fewer ongoing replicatons.
 * \param servers[out] - list of chunkservers for replication.
 * \param totalMatching[out] - number of existing chunkservers that matched the requested label.
 * \param returnedMatching[out] - number of returned chunkservers that matched the requested label.
 * \return Number of valid entries in \p servers.
 */
void matocsserv_getservers_lessrepl(const MediaLabel &label, uint32_t min_chunkserver_version,
		uint16_t replication_write_limit, const IpCounter &ip_counter,
		std::vector<matocsserventry*> &servers,
		int &total_matching, int &returned_matching, int &temporarily_unavailable);

/*! \brief Get chunkserver's label. */
const MediaLabel& matocsserv_get_label(matocsserventry* e);

/*! \brief Get chunkserver's disk usage. */
double matocsserv_get_usage(matocsserventry* e);

/*! \brief Get chunkservers ordered by disk usage. */
std::vector<ServerWithUsage> matocsserv_getservers_sorted();

uint32_t matocsserv_get_version(matocsserventry* e);
void matocsserv_usagedifference(double* minusage, double* maxusage,
		uint16_t* usablescount, uint16_t* totalscount);
std::vector<std::pair<matocsserventry*, ChunkPartType>> matocsserv_getservers_for_new_chunk(
		uint8_t goalId, uint32_t min_server_version = 0);
void matocsserv_getspace(uint64_t* totalspace, uint64_t* availspace);
const char* matocsserv_getstrip(matocsserventry* e);
uint32_t matocsserv_get_servip(matocsserventry *e);
int matocsserv_getlocation(matocsserventry* e, uint32_t* servip, uint16_t* servport,
		MediaLabel* label);
uint16_t matocsserv_replication_read_counter(matocsserventry* e);
uint16_t matocsserv_replication_write_counter(matocsserventry* e);
uint16_t matocsserv_deletion_counter(matocsserventry* e);
int matocsserv_send_replicatechunk(matocsserventry* e,
		uint64_t chunkid, uint32_t version, matocsserventry* src);
int matocsserv_send_liz_replicatechunk(matocsserventry* e,
		uint64_t chunkid, uint32_t version, ChunkPartType type,
		const std::vector<matocsserventry*> &sourcePointers,
		const std::vector<ChunkPartType> &sourceTypes);
int matocsserv_send_chunkop(matocsserventry* e,
		uint64_t chunkid, uint32_t version, uint32_t newversion,
		uint64_t copychunkid, uint32_t copyversion, uint32_t leng);
int matocsserv_send_deletechunk(matocsserventry* e,
		uint64_t chunkId, uint32_t chunkVersion, ChunkPartType chunkType);
int matocsserv_send_createchunk(matocsserventry* e,
		uint64_t chunkid, ChunkPartType chunkType, uint32_t version);
int matocsserv_send_setchunkversion(matocsserventry* e,
		uint64_t chunkId, uint32_t newVersion, uint32_t chunkVersion, ChunkPartType chunkType);
int matocsserv_send_duplicatechunk(matocsserventry* e,
		uint64_t newChunkId, uint32_t newChunkVersion,
		ChunkPartType chunkType, uint64_t chunkId, uint32_t chunkVersion);
void matocsserv_send_truncatechunk(matocsserventry* e,
		uint64_t chunkid, ChunkPartType chunkType, uint32_t length,
		uint32_t version, uint32_t oldversion);
int matocsserv_send_duptruncchunk(matocsserventry* e,
		uint64_t newChunkId, uint32_t newChunkVersion,
		ChunkPartType chunkType, uint64_t chunkId, uint32_t chunkVersion, uint32_t length);
int matocsserv_init(void);
void matocsserv_getserverdata(const matocsserventry* s, ChunkserverListEntry &result);
csdbentry *matocsserv_get_csdb(matocsserventry* s);
