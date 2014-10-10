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

struct matocsserventry;

int matocsserv_csdb_remove_server(uint32_t ip, uint16_t port);
void matocsserv_remove_server(matocsserventry* ptr);
void matocsserv_usagedifference(double* minusage, double* maxusage,
		uint16_t* usablescount, uint16_t* totalscount);
uint16_t matocsserv_getservers_ordered(matocsserventry* ptrs[65535],
		double maxusagediff, uint32_t* min, uint32_t* max);
std::vector<matocsserventry*> matocsserv_getservers_for_new_chunk(uint8_t desiredGoal);
uint16_t matocsserv_getservers_lessrepl(matocsserventry* ptrs[65535], uint16_t replimit);
void matocsserv_getspace(uint64_t* totalspace, uint64_t* availspace);
const char* matocsserv_getstrip(matocsserventry* e);
int matocsserv_getlocation(matocsserventry* e, uint32_t* servip, uint16_t* servport);
uint16_t matocsserv_replication_read_counter(matocsserventry* e);
uint16_t matocsserv_replication_write_counter(matocsserventry* e);
uint16_t matocsserv_deletion_counter(matocsserventry* e);
uint32_t matocsserv_cservlist_size(void);
void matocsserv_cservlist_data(uint8_t* ptr);
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
