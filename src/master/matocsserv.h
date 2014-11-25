/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

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

#include "common/media_label.h"
#include "common/chunkserver_list_entry.h"

/// A struct representing a chunkserver.
struct matocsserventry;

/// A list of chunkservers.
typedef std::vector<matocsserventry*> Chunkservers;

/// A struct used in matocsserv_getservers_sorted
struct ServerWithUsage {
	ServerWithUsage(matocsserventry* server, double diskUsage, const MediaLabel* label)
			: server(server),
			  diskUsage(diskUsage),
			  label(label) {
	}

	matocsserventry* server;
	double diskUsage;
	const MediaLabel* label;
};


/*! \brief Get list of chunkservers for replication with the given label.
 *
 * This function returns a list of chunkservers that currently don't exceed the given limit of
 * chunks replicated into them. Servers with 99% disk usage are treated as non-existing, thus not
 * returned. The returned servers are randomly shuffled, but if the \p label is not a
 * \p kMediaLabelWildcard, then servers with this label would be placed in front of the returned
 * list and \p returnedMatching would be set to the number of them.
 *
 * \param label - the requested label.
 * \param replicationWriteLimit - return only chunkservers with fewer ongoing replicatons.
 * \param servers[out] - list of chunkservers for replication.
 * \param totalMatching[out] - number of existing chunkservers that matched the requested label.
 * \param returnedMatching[out] - number of returned chunkservers that matched the requested label.
 * \return Number of valid entries in \p servers.
 */
uint16_t matocsserv_getservers_lessrepl(const MediaLabel& label, uint16_t replicationWriteLimit,
		matocsserventry* servers[65535], uint16_t* totalMatching, uint16_t* returnedMatching);

/*! \brief Get chunkserver's label. */
const MediaLabel& matocsserv_get_label(matocsserventry* e);

/*! \brief Get chunkserver's disk usage. */
double matocsserv_get_usage(matocsserventry* e);

/*! \brief Get chunkservers ordered by disk usage. */
std::vector<ServerWithUsage> matocsserv_getservers_sorted();

/*! \brief Get information about all chunkservers.
 *
 * This list includes disconnected chunkservers.
 * Disconnected chunkservers have the following fields set to non-zero:
 * \p version (set to \p kDisconnectedChunkserverVersion), \p servip, \p servport.
 */
std::vector<ChunkserverListEntry> matocsserv_cservlist();

int matocsserv_csdb_remove_server(uint32_t ip, uint16_t port);
void matocsserv_remove_server(matocsserventry* ptr);
void matocsserv_usagedifference(double* minusage, double* maxusage,
		uint16_t* usablescount, uint16_t* totalscount);
Chunkservers matocsserv_getservers_for_new_chunk(uint8_t desiredGoal);
void matocsserv_getspace(uint64_t* totalspace, uint64_t* availspace);
const char* matocsserv_getstrip(matocsserventry* e);
int matocsserv_getlocation(matocsserventry* e, uint32_t* servip, uint16_t* servport,
		MediaLabel** label);
uint16_t matocsserv_replication_read_counter(matocsserventry* e);
uint16_t matocsserv_replication_write_counter(matocsserventry* e);
uint16_t matocsserv_deletion_counter(matocsserventry* e);
int matocsserv_send_replicatechunk(matocsserventry* e,
		uint64_t chunkid, uint32_t version, matocsserventry* src);
int matocsserv_send_replicatechunk_xor(matocsserventry* e,
		uint64_t chunkid, uint32_t version, uint8_t cnt,
		void* *src, uint64_t* srcchunkid, uint32_t* srcversion);
int matocsserv_send_chunkop(matocsserventry* e,
		uint64_t chunkid, uint32_t version, uint32_t newversion,
		uint64_t copychunkid, uint32_t copyversion, uint32_t leng);
int matocsserv_send_deletechunk(matocsserventry* e, uint64_t chunkid, uint32_t version);
int matocsserv_send_createchunk(matocsserventry* e, uint64_t chunkid, uint32_t version);
int matocsserv_send_setchunkversion(matocsserventry* e,
		uint64_t chunkid, uint32_t version, uint32_t oldversion);
int matocsserv_send_duplicatechunk(matocsserventry* e,
		uint64_t chunkid, uint32_t version, uint64_t oldchunkid, uint32_t oldversion);
int matocsserv_send_truncatechunk(matocsserventry* e,
		uint64_t chunkid, uint32_t length, uint32_t version, uint32_t oldversion);
int matocsserv_send_duptruncchunk(matocsserventry* e,
		uint64_t chunkid, uint32_t version,
		uint64_t oldchunkid, uint32_t oldversion, uint32_t length);
int matocsserv_init(void);
