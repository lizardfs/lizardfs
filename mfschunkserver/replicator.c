/*
   Copyright 2008 Gemius SA.

   This file is part of MooseFS.

   MooseFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   MooseFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with MooseFS.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <inttypes.h>
#include <stdlib.h>

#include "MFSCommunication.h"

#include "config.h"
#include "hddspacemgr.h"
#include "cstocsconn.h"
#include "masterconn.h"

enum {FREE,CONNECTING,REPLICATING};

typedef struct replication {
	uint64_t chunkid;
	uint32_t version;
	uint8_t mode;
	void *conn;
} replication;

static uint32_t stats_repl=0;

void replicator_stats(uint32_t *repl) {
	*repl = stats_repl;
	stats_repl=0;
}

void replicator_new(uint64_t chunkid,uint32_t version,uint32_t ip,uint16_t port) {
	replication *eptr;
	uint8_t status;
	stats_repl++;
	status = create_newchunk(chunkid,0);	//create version 0 - change it after replication
	if (status!=STATUS_OK) {
		masterconn_replicate_status(chunkid,version,status);
		return;
	}
	eptr = malloc(sizeof(replication));
	if (eptr==NULL) {
		delete_chunk(chunkid,0);
		masterconn_replicate_status(chunkid,version,ERROR_OUTOFMEMORY);
		return;
	}
	eptr->chunkid = chunkid;
	eptr->version = version;
	eptr->mode = CONNECTING;
	eptr->conn = NULL;
	if (cstocsconn_newreplconnection(ip,port,eptr)==0) {
		free(eptr);
		delete_chunk(chunkid,0);	// ignore status
		masterconn_replicate_status(chunkid,version,ERROR_CANTCONNECT);
		return;
	}
}

void replicator_cstocs_gotdata(void *e,uint64_t chunkid,uint16_t blocknum,uint16_t offset,uint32_t size,uint32_t crc,uint8_t *ptr) {
	replication *eptr = (replication*)e;
	uint8_t status;
	if (eptr->mode!=REPLICATING) {
		return;
	}
	if (chunkid!=eptr->chunkid) {
		chunk_after_io(eptr->chunkid);	//proforma
		delete_chunk(chunkid,0);	// ignore status
		masterconn_replicate_status(chunkid,eptr->version,ERROR_WRONGCHUNKID);
		cstocsconn_delete(eptr->conn);
		free(eptr);
	}
	status = write_block_to_chunk(chunkid,0,blocknum,ptr,offset,size,crc);
	if (status!=STATUS_OK) {
		chunk_after_io(eptr->chunkid);	//proforma
		delete_chunk(chunkid,0);
		masterconn_replicate_status(chunkid,eptr->version,status);
		cstocsconn_delete(eptr->conn);
		free(eptr);
	}
}

void replicator_cstocs_gotstatus(void *e,uint64_t chunkid,uint8_t s) {
	replication *eptr = (replication*)e;
	uint8_t status;
	if (eptr->mode!=REPLICATING) {
		chunk_after_io(eptr->chunkid);	//proforma
		delete_chunk(eptr->chunkid,0);
		masterconn_replicate_status(eptr->chunkid,eptr->version,ERROR_DISCONNECTED);
		cstocsconn_delete(eptr->conn);
		free(eptr);
		return;
	}
	if (chunkid!=eptr->chunkid) {
		chunk_after_io(eptr->chunkid);	//proforma
		delete_chunk(eptr->chunkid,0);	// ignore status
		masterconn_replicate_status(eptr->chunkid,eptr->version,ERROR_WRONGCHUNKID);
		cstocsconn_delete(eptr->conn);
		free(eptr);
		return;
	}
	if (s!=STATUS_OK) {
		chunk_after_io(eptr->chunkid);	//proforma
		delete_chunk(eptr->chunkid,0);
		masterconn_replicate_status(eptr->chunkid,eptr->version,s);
		cstocsconn_delete(eptr->conn);
		free(eptr);
		return;
	}
	status = chunk_after_io(eptr->chunkid);
	if (status!=STATUS_OK) {
		delete_chunk(eptr->chunkid,0);
		masterconn_replicate_status(eptr->chunkid,eptr->version,status);
		cstocsconn_delete(eptr->conn);
		free(eptr);
		return;
	}
	status = set_chunk_version(chunkid,eptr->version,0);
	if (status!=STATUS_OK) {
		delete_chunk(chunkid,0);
		masterconn_replicate_status(chunkid,eptr->version,status);
	} else {
		masterconn_replicate_status(chunkid,eptr->version,STATUS_OK);
	}
	cstocsconn_delete(eptr->conn);
	free(eptr);
}

void replicator_cstocs_connected(void *e,void *cptr) {
	replication *eptr = (replication*)e;
	uint8_t status;
	eptr->conn = cptr;
	if (eptr->mode!=CONNECTING) {
		delete_chunk(eptr->chunkid,0);
		masterconn_replicate_status(eptr->chunkid,eptr->version,ERROR_DISCONNECTED);
		cstocsconn_delete(eptr->conn);
		free(eptr);
		return;
	}
	status = chunk_before_io(eptr->chunkid);
	if (status!=STATUS_OK) {
		delete_chunk(eptr->chunkid,0);
		masterconn_replicate_status(eptr->chunkid,eptr->version,status);
		cstocsconn_delete(eptr->conn);
		free(eptr);
		return;
	}
	cstocsconn_replinit(eptr->conn,eptr->chunkid,eptr->version);
	eptr->mode = REPLICATING;
}

void replicator_cstocs_disconnected(void *e) {
	replication *eptr = (replication*)e;
	if (eptr->conn) {	// do it only on connected records
		chunk_after_io(eptr->chunkid);	//proforma
	}
	delete_chunk(eptr->chunkid,0);
	masterconn_replicate_status(eptr->chunkid,eptr->version,ERROR_DISCONNECTED);
	free(eptr);
}
