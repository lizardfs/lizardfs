/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2015 Skytechnology sp. z o.o..

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

#include "common/platform.h"
#include "chunkserver/legacy_replicator.h"

#include <errno.h>
#include <inttypes.h>
#include <poll.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <syslog.h>
#include <unistd.h>

#include "chunkserver/g_limiters.h"
#include "chunkserver/hddspacemgr.h"
#include "common/crc.h"
#include "common/datapack.h"
#include "common/massert.h"
#include "protocol/MFSCommunication.h"
#include "common/mfserr.h"
#include "common/slogger.h"
#include "common/sockets.h"

#define CONNMSECTO 5000
#define SENDMSECTO 5000
#define RECVMSECTO 5000

#define MAX_RECV_PACKET_SIZE (20+MFSBLOCKSIZE)

typedef enum {IDLE,CONNECTING,HEADER,DATA} modetype;

typedef struct _repsrc {
	int sock;
	modetype mode;
	uint8_t hdrbuff[8];
	uint8_t *packet;
	uint8_t *startptr;
	uint32_t bytesleft;

	uint64_t chunkid;
	uint32_t version;
	uint16_t blocks;

	uint32_t ip;
	uint16_t port;
} repsrc;

typedef struct _replication {
	uint64_t chunkid;
	uint32_t version;

	uint8_t *xorbuff;

	uint8_t created,opened;
	uint8_t srccnt;
	struct pollfd *fds;
	repsrc *repsources;
} replication;

static uint32_t stats_repl=0;
static pthread_mutex_t statslock = PTHREAD_MUTEX_INITIALIZER;

void legacy_replicator_stats(uint32_t *repl) {
	pthread_mutex_lock(&statslock);
	*repl = stats_repl;
	stats_repl=0;
	pthread_mutex_unlock(&statslock);
}

static int rep_read(repsrc *rs) {
	int32_t i;
	uint32_t size;
	const uint8_t *ptr;
	while (rs->bytesleft>0) {
		i=read(rs->sock,rs->startptr,rs->bytesleft);
		if (i==0) {
			syslog(LOG_NOTICE,"replicator: connection lost");
			return -1;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE,"replicator: read error");
				return -1;
			}
			return 0;
		}
		rs->startptr+=i;
		rs->bytesleft-=i;

		if (rs->bytesleft>0) {
			return 0;
		}

		if (rs->mode==HEADER) {
			ptr = rs->hdrbuff+4;
			size = get32bit(&ptr);

			if (rs->packet) {
				free(rs->packet);
			}
			if (size>0) {
				if (size>MAX_RECV_PACKET_SIZE) {
					syslog(LOG_WARNING,"replicator: packet too long (%" PRIu32 "/%u)",size,MAX_RECV_PACKET_SIZE);
					return -1;
				}
				rs->packet = (uint8_t*) malloc(size);
				passert(rs->packet);
				rs->startptr = rs->packet;
			} else {
				rs->packet = NULL;
			}
			rs->bytesleft = size;
			rs->mode = DATA;
		}
	}
	return 0;
}

static int rep_receive_all_packets(replication *r,uint32_t msecto) {
	uint8_t i,l;
	struct timeval tvb,tv;
	uint32_t msec;
	gettimeofday(&tvb,NULL);
	for (;;) {
		l=1;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].bytesleft>0) {
				r->fds[i].events = POLLIN;
				l=0;
			} else {
				r->fds[i].events = 0;
			}
		}
		if (l) {        // finished
			return 0;
		}
		gettimeofday(&tv,NULL);
		if (tv.tv_usec < tvb.tv_usec) {
			tv.tv_usec+=1000000;
			tv.tv_sec--;
		}
		tv.tv_sec-=tvb.tv_sec;
		tv.tv_usec-=tvb.tv_usec;
		msec = tv.tv_sec * 1000 + tv.tv_usec / 1000;
		if (msec>=msecto) {
			syslog(LOG_NOTICE,"replicator: receive timed out");
			return -1; // timed out
		}
		if (poll(r->fds,r->srccnt,msecto-msec)<0) {
			if (errno!=EINTR && errno!=EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE,"replicator: poll error");
				return -1;
			}
			continue;
		}
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->fds[i].revents & POLLHUP) {
				syslog(LOG_NOTICE,"replicator: connection lost");
				return -1;
			}
			if (r->fds[i].revents & POLLIN) {
				if (rep_read(r->repsources+i)<0) {
					return -1;
				}
			}
		}
	}
}

static uint8_t* rep_create_packet(repsrc *rs,uint32_t type,uint32_t size) {
	uint8_t *ptr;
	if (rs->packet) {
		free(rs->packet);
	}
	rs->packet = (uint8_t*) malloc(size+8);
	passert(rs->packet);
	ptr = rs->packet;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	rs->startptr = rs->packet;
	rs->bytesleft = 8+size;
	return ptr;
}

static void rep_no_packet(repsrc *rs) {
	if (rs->packet) {
		free(rs->packet);
	}
	rs->packet=NULL;
	rs->startptr=NULL;
	rs->bytesleft=0;
}

static int rep_write(repsrc *rs) {
	int i;
	i = write(rs->sock,rs->startptr,rs->bytesleft);
	if (i==0) {
		syslog(LOG_NOTICE,"replicator: connection lost");
		return -1;
	}
	if (i<0) {
		if (errno!=EAGAIN) {
			lzfs_silent_errlog(LOG_NOTICE,"replicator: write error");
			return -1;
		}
		return 0;
	}
	rs->startptr+=i;
	rs->bytesleft-=i;
	return 0;
}

static int rep_send_all_packets(replication *r,uint32_t msecto) {
	uint8_t i,l;
	struct timeval tvb,tv;
	uint32_t msec;
	gettimeofday(&tvb,NULL);
	for (;;) {
		l=1;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].bytesleft>0) {
				r->fds[i].events = POLLOUT;
				l=0;
			} else {
				r->fds[i].events = 0;
			}
		}
		if (l) {        // finished
			return 0;
		}
		gettimeofday(&tv,NULL);
		if (tv.tv_usec < tvb.tv_usec) {
			tv.tv_usec+=1000000;
			tv.tv_sec--;
		}
		tv.tv_sec-=tvb.tv_sec;
		tv.tv_usec-=tvb.tv_usec;
		msec = tv.tv_sec * 1000 + tv.tv_usec / 1000;
		if (msec>=msecto) {
			syslog(LOG_NOTICE,"replicator: send timed out");
			return -1; // timed out
		}
		if (poll(r->fds,r->srccnt,msecto-msec)<0) {
			if (errno!=EINTR && errno!=EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE,"replicator: poll error");
				return -1;
			}
			continue;
		}
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->fds[i].revents & POLLHUP) {
				syslog(LOG_NOTICE,"replicator: connection lost");
				return -1;
			}
			if (r->fds[i].revents & POLLOUT) {
				if (rep_write(r->repsources+i)<0) {
					return -1;
				}
			}
		}
	}
}

static int rep_wait_for_connection(replication *r,uint32_t msecto) {
	uint8_t i,l;
	struct timeval tvb,tv;
	uint32_t msec;
	gettimeofday(&tvb,NULL);
	for (;;) {
		l=1;
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->repsources[i].mode==CONNECTING) {
				r->fds[i].events = POLLOUT;
				l=0;
			} else {
				r->fds[i].events = 0;
			}
		}
		if (l) {        // finished
			return 0;
		}
		gettimeofday(&tv,NULL);
		if (tv.tv_usec < tvb.tv_usec) {
			tv.tv_usec+=1000000;
			tv.tv_sec--;
		}
		tv.tv_sec-=tvb.tv_sec;
		tv.tv_usec-=tvb.tv_usec;
		msec = tv.tv_sec * 1000 + tv.tv_usec / 1000;
		if (msec>=msecto) {
			syslog(LOG_NOTICE,"replicator: connect timed out");
			return -1; // timed out
		}
		if (poll(r->fds,r->srccnt,msecto-msec)<0) {
			if (errno!=EINTR && errno!=EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE,"replicator: poll error");
				return -1;
			}
			continue;
		}
		for (i=0 ; i<r->srccnt ; i++) {
			if (r->fds[i].revents & POLLHUP) {
				syslog(LOG_NOTICE,"replicator: connection lost");
				return -1;
			}
			if (r->fds[i].revents & POLLOUT) {
				if (tcpgetstatus(r->repsources[i].sock)<0) {
					lzfs_silent_errlog(LOG_NOTICE,"replicator: connect error");
					return -1;
				}
				r->repsources[i].mode=IDLE;
			}
		}
	}
}

static void rep_cleanup(replication *r) {
	int i;
	if (r->opened) {
		hdd_close(r->chunkid, slice_traits::standard::ChunkPartType());
	}
	if (r->created) {
		hdd_delete(r->chunkid, 0, slice_traits::standard::ChunkPartType());
	}
	for (i=0 ; i<r->srccnt ; i++) {
		if (r->repsources[i].sock>=0) {
			tcpclose(r->repsources[i].sock);
		}
		if (r->repsources[i].packet) {
			free(r->repsources[i].packet);
		}
	}
	if (r->fds) {
		free(r->fds);
	}
	if (r->repsources) {
		free(r->repsources);
	}
	if (r->xorbuff) {
		free(r->xorbuff);
	}
}

/* srcs: srccnt * (chunkid:64 version:32 ip:32 port:16) */
uint8_t legacy_replicate(uint64_t chunkid,uint32_t version,uint8_t srccnt,const uint8_t *srcs) {
	replication r;
	uint8_t status, i, vbuffs;
	uint16_t b, blocks;
	uint8_t *wptr;
	const uint8_t *rptr;
	int s;

	if (srccnt==0) {
		return LIZARDFS_ERROR_EINVAL;
	}

//      syslog(LOG_NOTICE,"replication begin (chunkid:%08" PRIX64 ",version:%04" PRIX32 ",srccnt:%" PRIu8 ")",chunkid,version,srccnt);

	pthread_mutex_lock(&statslock);
	stats_repl++;
	pthread_mutex_unlock(&statslock);

// init replication structure
	r.chunkid = chunkid;
	r.version = version;
	r.srccnt = 0;
	r.created = 0;
	r.opened = 0;
	r.fds = (pollfd*) malloc(sizeof(struct pollfd)*srccnt);
	passert(r.fds);
	r.repsources = (repsrc*) malloc(sizeof(repsrc)*srccnt);
	passert(r.repsources);
	if (srccnt>1) {
		r.xorbuff = (uint8_t*) malloc(MFSBLOCKSIZE+4);
		passert(r.xorbuff);
	} else {
		r.xorbuff = NULL;
	}
// create chunk
	status = hdd_create(chunkid, 0, slice_traits::standard::ChunkPartType());
	if (status!=LIZARDFS_STATUS_OK) {
		syslog(LOG_NOTICE,"replicator: hdd_create status: %s",lizardfs_error_string(status));
		rep_cleanup(&r);
		return status;
	}
	r.created = 1;
// init sources
	r.srccnt = srccnt;
	for (i=0 ; i<srccnt ; i++) {
		r.repsources[i].chunkid = get64bit(&srcs);
		r.repsources[i].version = get32bit(&srcs);
		r.repsources[i].ip = get32bit(&srcs);
		r.repsources[i].port = get16bit(&srcs);
		r.repsources[i].sock = -1;
		r.repsources[i].packet = NULL;
	}
// connect
	for (i=0 ; i<srccnt ; i++) {
		s = tcpsocket();
		if (s<0) {
			lzfs_silent_errlog(LOG_NOTICE,"replicator: socket error");
			rep_cleanup(&r);
			return LIZARDFS_ERROR_CANTCONNECT;
		}
		r.repsources[i].sock = s;
		r.fds[i].fd = s;
		if (tcpnonblock(s)<0) {
			lzfs_silent_errlog(LOG_NOTICE,"replicator: nonblock error");
			rep_cleanup(&r);
			return LIZARDFS_ERROR_CANTCONNECT;
		}
		s = tcpnumconnect(s,r.repsources[i].ip,r.repsources[i].port);
		if (s<0) {
			lzfs_silent_errlog(LOG_NOTICE,"replicator: connect error");
			rep_cleanup(&r);
			return LIZARDFS_ERROR_CANTCONNECT;
		}
		if (s==0) {
			r.repsources[i].mode = IDLE;
		} else {
			r.repsources[i].mode = CONNECTING;
		}
	}
	if (rep_wait_for_connection(&r,CONNMSECTO)<0) {
		rep_cleanup(&r);
		return LIZARDFS_ERROR_CANTCONNECT;
	}
// open chunk
	status = hdd_open(chunkid, slice_traits::standard::ChunkPartType());
	if (status!=LIZARDFS_STATUS_OK) {
		syslog(LOG_NOTICE,"replicator: hdd_open status: %s",lizardfs_error_string(status));
		rep_cleanup(&r);
		return status;
	}
	r.opened = 1;
// get block numbers
	for (i=0 ; i<srccnt ; i++) {
		wptr = rep_create_packet(r.repsources+i,CSTOCS_GET_CHUNK_BLOCKS,8+4);
		if (wptr==NULL) {
			syslog(LOG_NOTICE,"replicator: out of memory");
			rep_cleanup(&r);
			return LIZARDFS_ERROR_OUTOFMEMORY;
		}
		put64bit(&wptr,r.repsources[i].chunkid);
		put32bit(&wptr,r.repsources[i].version);
	}
// send packet
	if (rep_send_all_packets(&r,SENDMSECTO)<0) {
		rep_cleanup(&r);
		return LIZARDFS_ERROR_DISCONNECTED;
	}
// receive answers
	for (i=0 ; i<srccnt ; i++) {
		r.repsources[i].mode = HEADER;
		r.repsources[i].startptr = r.repsources[i].hdrbuff;
		r.repsources[i].bytesleft = 8;
	}
	if (rep_receive_all_packets(&r,RECVMSECTO)<0) {
		rep_cleanup(&r);
		return LIZARDFS_ERROR_DISCONNECTED;
	}
// get block no
	blocks = 0;
	for (i=0 ; i<srccnt ; i++) {
		uint32_t type,size;
		uint64_t pchid;
		uint32_t pver;
		uint16_t pblocks;
		uint8_t pstatus;
		rptr = r.repsources[i].hdrbuff;
		type = get32bit(&rptr);
		size = get32bit(&rptr);
		rptr = r.repsources[i].packet;
		if (rptr==NULL || type!=CSTOCS_GET_CHUNK_BLOCKS_STATUS || size!=15) {
			syslog(LOG_WARNING,"replicator: got wrong answer (type/size) from (%08" PRIX32 ":%04" PRIX16 ")",r.repsources[i].ip,r.repsources[i].port);
			rep_cleanup(&r);
			return LIZARDFS_ERROR_DISCONNECTED;
		}
		pchid = get64bit(&rptr);
		pver = get32bit(&rptr);
		pblocks = get16bit(&rptr);
		pstatus = get8bit(&rptr);
		if (pchid!=r.repsources[i].chunkid) {
			syslog(LOG_WARNING,"replicator: got wrong answer (chunk_status:chunkid:%" PRIX64 "/%" PRIX64 ") from (%08" PRIX32 ":%04" PRIX16 ")",pchid,r.repsources[i].chunkid,r.repsources[i].ip,r.repsources[i].port);
			rep_cleanup(&r);
			return LIZARDFS_ERROR_WRONGCHUNKID;
		}
		if (pver!=r.repsources[i].version) {
			syslog(LOG_WARNING,"replicator: got wrong answer (chunk_status:version:%" PRIX32 "/%" PRIX32 ") from (%08" PRIX32 ":%04" PRIX16 ")",pver,r.repsources[i].version,r.repsources[i].ip,r.repsources[i].port);
			rep_cleanup(&r);
			return LIZARDFS_ERROR_WRONGVERSION;
		}
		if (pstatus!=LIZARDFS_STATUS_OK) {
			syslog(LOG_NOTICE,"replicator: got status: %s from (%08" PRIX32 ":%04" PRIX16 ")",
			       lizardfs_error_string(pstatus),r.repsources[i].ip,r.repsources[i].port);
			rep_cleanup(&r);
			return pstatus;
		}
		r.repsources[i].blocks = pblocks;
		if (pblocks>blocks) {
			blocks=pblocks;
		}
	}
// create read request
	uint32_t requestsSummaryLength = 0;
	for (i=0 ; i<srccnt ; i++) {
		if (r.repsources[i].blocks>0) {
			uint32_t leng;
			wptr = rep_create_packet(r.repsources+i,CLTOCS_READ,8+4+4+4);
			if (wptr==NULL) {
				syslog(LOG_NOTICE,"replicator: out of memory");
				rep_cleanup(&r);
				return LIZARDFS_ERROR_OUTOFMEMORY;
			}
			leng = r.repsources[i].blocks*MFSBLOCKSIZE;
			requestsSummaryLength += leng;
			put64bit(&wptr,r.repsources[i].chunkid);
			put32bit(&wptr,r.repsources[i].version);
			put32bit(&wptr,0);
			put32bit(&wptr,leng);
		} else {
			rep_no_packet(r.repsources+i);
		}
	}
// wait for replication bandwidth limit to be assigned
	status = replicationBandwidthLimiter().wait(requestsSummaryLength, std::chrono::seconds(60));
	if (status != LIZARDFS_STATUS_OK) {
		syslog(LOG_WARNING, "Replication bandwidth limit error: %s", lizardfs_error_string(status));
		return status;
	}
// send read request
	if (rep_send_all_packets(&r,SENDMSECTO)<0) {
		rep_cleanup(&r);
		return LIZARDFS_ERROR_DISCONNECTED;
	}
// receive data and write to hdd
	for (b=0 ; b<blocks ; b++) {
// prepare receive
		for (i=0 ; i<srccnt ; i++) {
			if (b<r.repsources[i].blocks) {
				r.repsources[i].mode = HEADER;
				r.repsources[i].startptr = r.repsources[i].hdrbuff;
				r.repsources[i].bytesleft = 8;
			} else {
				r.repsources[i].mode = IDLE;
				r.repsources[i].bytesleft = 0;
			}
		}
// receive data
		if (rep_receive_all_packets(&r,RECVMSECTO)<0) {
			rep_cleanup(&r);
			return LIZARDFS_ERROR_DISCONNECTED;
		}
// check packets
		vbuffs = 0;
		uint32_t crc = 0;
		for (i=0 ; i<srccnt ; i++) {
			if (r.repsources[i].mode!=IDLE) {
				uint32_t type,size;
				uint64_t pchid;
				uint16_t pblocknum;
				uint16_t poffset;
				uint32_t psize;
				uint8_t pstatus;
				rptr = r.repsources[i].hdrbuff;
				type = get32bit(&rptr);
				size = get32bit(&rptr);
				rptr = r.repsources[i].packet;
				if (rptr==NULL) {
					rep_cleanup(&r);
					return LIZARDFS_ERROR_DISCONNECTED;
				}
				if (type==CSTOCL_READ_STATUS && size==9) {
					pchid = get64bit(&rptr);
					pstatus = get8bit(&rptr);
					if (pchid!=r.repsources[i].chunkid) {
						syslog(LOG_WARNING,"replicator: got wrong answer (read_status:chunkid:%" PRIX64 "/%" PRIX64 ") from (%08" PRIX32 ":%04" PRIX16 ")",pchid,r.repsources[i].chunkid,r.repsources[i].ip,r.repsources[i].port);
						rep_cleanup(&r);
						return LIZARDFS_ERROR_WRONGCHUNKID;
					}
					if (pstatus==LIZARDFS_STATUS_OK) {       // got status too early or got incorrect packet
						syslog(LOG_WARNING,"replicator: got unexpected ok status from (%08" PRIX32 ":%04" PRIX16 ")",r.repsources[i].ip,r.repsources[i].port);
						rep_cleanup(&r);
						return LIZARDFS_ERROR_DISCONNECTED;
					}
					syslog(LOG_NOTICE,"replicator: got status: %s from (%08" PRIX32 ":%04" PRIX16 ")",
					       lizardfs_error_string(pstatus),r.repsources[i].ip,r.repsources[i].port);
					rep_cleanup(&r);
					return pstatus;
				} else if (type==CSTOCL_READ_DATA && size==20+MFSBLOCKSIZE) {
					pchid = get64bit(&rptr);
					pblocknum = get16bit(&rptr);
					poffset = get16bit(&rptr);
					psize = get32bit(&rptr);
					crc = get32bit(&rptr);
					if (pchid!=r.repsources[i].chunkid) {
						syslog(LOG_WARNING,"replicator: got wrong answer (read_data:chunkid:%" PRIX64 "/%" PRIX64 ") from (%08" PRIX32 ":%04" PRIX16 ")",pchid,r.repsources[i].chunkid,r.repsources[i].ip,r.repsources[i].port);
						rep_cleanup(&r);
						return LIZARDFS_ERROR_WRONGCHUNKID;
					}
					if (pblocknum!=b) {
						syslog(LOG_WARNING,"replicator: got wrong answer (read_data:blocknum:%" PRIu16 "/%" PRIu16 ") from (%08" PRIX32 ":%04" PRIX16 ")",pblocknum,b,r.repsources[i].ip,r.repsources[i].port);
						rep_cleanup(&r);
						return LIZARDFS_ERROR_DISCONNECTED;
					}
					if (poffset!=0) {
						syslog(LOG_WARNING,"replicator: got wrong answer (read_data:offset:%" PRIu16 ") from (%08" PRIX32 ":%04" PRIX16 ")",poffset,r.repsources[i].ip,r.repsources[i].port);
						rep_cleanup(&r);
						return LIZARDFS_ERROR_WRONGOFFSET;
					}
					if (psize!=MFSBLOCKSIZE) {
						syslog(LOG_WARNING,"replicator: got wrong answer (read_data:size:%" PRIu32 ") from (%08" PRIX32 ":%04" PRIX16 ")",psize,r.repsources[i].ip,r.repsources[i].port);
						rep_cleanup(&r);
						return LIZARDFS_ERROR_WRONGSIZE;
					}
				} else {
					syslog(LOG_WARNING,"replicator: got wrong answer (type/size) from (%08" PRIX32 ":%04" PRIX16 ")",r.repsources[i].ip,r.repsources[i].port);
					rep_cleanup(&r);
					return LIZARDFS_ERROR_DISCONNECTED;
				}
				vbuffs++;
			}
		}
// write data
		sassert(vbuffs <= 1); // xor not needed, so just find block and write it
		if (vbuffs==0) {        // no buffers ? - it should never happen
			syslog(LOG_WARNING,"replicator: no data received for block: %" PRIu16,b);
			rep_cleanup(&r);
			return LIZARDFS_ERROR_DISCONNECTED;
		} else {
			for (i=0 ; i<srccnt ; i++) {
				if (r.repsources[i].mode!=IDLE) {
					rptr = r.repsources[i].packet;
					status = hdd_write(chunkid, 0, slice_traits::standard::ChunkPartType(),
							b, 0, MFSBLOCKSIZE, crc, rptr + 20);
					if (status!=LIZARDFS_STATUS_OK) {
						syslog(LOG_WARNING,"replicator: write status: %s",lizardfs_error_string(status));
						rep_cleanup(&r);
						return status;
					}
				}
			}
		}
	}
// receive status
	for (i=0 ; i<srccnt ; i++) {
		if (r.repsources[i].blocks>0) {
			r.repsources[i].mode = HEADER;
			r.repsources[i].startptr = r.repsources[i].hdrbuff;
			r.repsources[i].bytesleft = 8;
		} else {
			r.repsources[i].mode = IDLE;
			r.repsources[i].bytesleft = 0;
		}
	}
	if (rep_receive_all_packets(&r,RECVMSECTO)<0) {
		rep_cleanup(&r);
		return LIZARDFS_ERROR_DISCONNECTED;
	}
	for (i=0 ; i<srccnt ; i++) {
		if (r.repsources[i].blocks>0) {
			uint32_t type,size;
			uint64_t pchid;
			uint8_t pstatus;
			rptr = r.repsources[i].hdrbuff;
			type = get32bit(&rptr);
			size = get32bit(&rptr);
			rptr = r.repsources[i].packet;
			if (rptr==NULL || type!=CSTOCL_READ_STATUS || size!=9) {
				syslog(LOG_WARNING,"replicator: got wrong answer (type/size) from (%08" PRIX32 ":%04" PRIX16 ")",r.repsources[i].ip,r.repsources[i].port);
				rep_cleanup(&r);
				return LIZARDFS_ERROR_DISCONNECTED;
			}
			pchid = get64bit(&rptr);
			pstatus = get8bit(&rptr);
			if (pchid!=r.repsources[i].chunkid) {
				syslog(LOG_WARNING,"replicator: got wrong answer (read_status:chunkid:%" PRIX64 "/%" PRIX64 ") from (%08" PRIX32 ":%04" PRIX16 ")",pchid,r.repsources[i].chunkid,r.repsources[i].ip,r.repsources[i].port);
				rep_cleanup(&r);
				return LIZARDFS_ERROR_WRONGCHUNKID;
			}
			if (pstatus!=LIZARDFS_STATUS_OK) {
				syslog(LOG_NOTICE,"replicator: got status: %s from (%08" PRIX32 ":%04" PRIX16 ")",
				       lizardfs_error_string(pstatus),r.repsources[i].ip,r.repsources[i].port);
				rep_cleanup(&r);
				return pstatus;
			}
		}
	}
// close chunk and change version
	status = hdd_close(chunkid, slice_traits::standard::ChunkPartType());
	if (status!=LIZARDFS_STATUS_OK) {
		syslog(LOG_NOTICE,"replicator: hdd_close status: %s",lizardfs_error_string(status));
		rep_cleanup(&r);
		return status;
	}
	r.opened = 0;
	status = hdd_version(chunkid, 0, slice_traits::standard::ChunkPartType(), version);
	if (status!=LIZARDFS_STATUS_OK) {
		syslog(LOG_NOTICE,"replicator: hdd_version status: %s",lizardfs_error_string(status));
		rep_cleanup(&r);
		return status;
	}
	r.created = 0;
	rep_cleanup(&r);
	return LIZARDFS_STATUS_OK;
}
