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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <syslog.h>

#include "MFSCommunication.h"
#include "sockets.h"
#include "datapack.h"
#include "crc.h"

#define CSMSECTIMEOUT 5000

int cs_readblock(int fd,uint64_t chunkid,uint32_t version,uint32_t offset,uint32_t size,uint8_t *buff) {
	uint8_t *ptr,ibuff[28];

	ptr = ibuff;
	PUT32BIT(CUTOCS_READ,ptr);
	PUT32BIT(20,ptr);
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
	PUT32BIT(offset,ptr);
	PUT32BIT(size,ptr);
	if (tcptowrite(fd,ibuff,28,CSMSECTIMEOUT)!=28) {
		syslog(LOG_NOTICE,"readblock; tcpwrite error: %m");
		return -1;
	}
	for (;;) {
		uint32_t cmd,l;
		uint64_t t64;
		uint16_t blockno,blockoffset;
		uint32_t breq,blocksize,blockcrc;
		if (tcptoread(fd,ibuff,8,CSMSECTIMEOUT)!=8) {
			syslog(LOG_NOTICE,"readblock; tcpread error: %m");
			return -1;
		}
		ptr = ibuff;
		GET32BIT(cmd,ptr);
		GET32BIT(l,ptr);
		if (cmd==CSTOCU_READ_STATUS) {
			if (l!=9) {
				syslog(LOG_NOTICE,"readblock; READ_STATUS incorrect message size (%"PRIu32"/9)",l);
				return -1;
			}
			if (tcptoread(fd,ibuff,9,CSMSECTIMEOUT)!=9) {
				syslog(LOG_NOTICE,"readblock; READ_STATUS tcpread error: %m");
				return -1;
			}
			ptr = ibuff;
			GET64BIT(t64,ptr);
			if (*ptr!=0) {
				syslog(LOG_NOTICE,"readblock; READ_STATUS status: %"PRIu8,*ptr);
				return -1;
			}
			if (t64!=chunkid) {
				syslog(LOG_NOTICE,"readblock; READ_STATUS incorrect chunkid (got:%"PRIu64" expected:%"PRIu64")",t64,chunkid);
				return -1;
			}
			if (size!=0) {
				syslog(LOG_NOTICE,"readblock; READ_STATUS incorrect data size (left: %"PRIu32")",size);
				return -1;
			}
			return 0;
		} else if (cmd==CSTOCU_READ_DATA) {
			if (l<20) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect message size (%"PRIu32"/>=20)",l);
				return -1;
			}
			if (tcptoread(fd,ibuff,20,CSMSECTIMEOUT)!=20) {
				syslog(LOG_NOTICE,"readblock; READ_DATA tcpread error: %m");
				return -1;
			}
			ptr = ibuff;
			GET64BIT(t64,ptr);
			if (t64!=chunkid) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect chunkid (got:%"PRIu64" expected:%"PRIu64")",t64,chunkid);
				return -1;
			}
			GET16BIT(blockno,ptr);
			GET16BIT(blockoffset,ptr);
			GET32BIT(blocksize,ptr);
			GET32BIT(blockcrc,ptr);
			if (l!=20+blocksize) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect message size (%"PRIu32"/%"PRIu32")",l,20+blocksize);
				return -1;
			}
			if (blocksize==0) {
				syslog(LOG_NOTICE,"readblock; READ_DATA empty block");
				return -1;
			}
			if (blockno!=(offset>>16)) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect block number (got:%"PRIu16" expected:%"PRIu32")",blockno,(offset>>16));
				return -1;
			}
			if (blockoffset!=(offset&0xFFFF)) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect block offset (got:%"PRIu16" expected:%"PRIu32")",blockoffset,(offset&0xFFFF));
				return -1;
			}
			breq = 65536 - (uint32_t)blockoffset;
			if (size<breq) {
				breq=size;
			}
			if (blocksize!=breq) {
				syslog(LOG_NOTICE,"readblock; READ_DATA incorrect block size (got:%"PRIu32" expected:%"PRIu32")",blocksize,breq);
				return -1;
			}
			if (tcptoread(fd,buff,blocksize,CSMSECTIMEOUT)!=(int32_t)blocksize) {
				syslog(LOG_NOTICE,"readblock; READ_DATA tcpread error: %m");
				return -1;
			}
			if (blockcrc!=crc32(0,buff,blocksize)) {
				syslog(LOG_NOTICE,"readblock; READ_DATA crc checksum error");
				return -1;
			}
			offset+=blocksize;
			size-=blocksize;
			buff+=blocksize;
		} else {
			syslog(LOG_NOTICE,"readblock; unknown message");
			return -1;
		}
	}
	return 0;
}

int cs_writestatus(int fd,uint64_t chunkid,uint32_t writeid) {
	uint8_t *ptr,ibuff[21];
	uint32_t t32;
	uint64_t t64;
	if (tcptoread(fd,ibuff,21,CSMSECTIMEOUT)!=21) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS tcpread error: %m");
		return -1;
	}
	ptr = ibuff;
	GET32BIT(t32,ptr);
	if (t32!=CSTOCU_WRITE_STATUS) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS unknown message (%"PRIu32")",t32);
		return -1;
	}
	GET32BIT(t32,ptr);
	if (t32!=13) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS incorrect message size (%"PRIu32"/13)",t32);
		return -1;
	}
	GET64BIT(t64,ptr);
	GET32BIT(t32,ptr);
	if (*ptr!=0) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS status: %"PRIu8,*ptr);
		return -1;
	}
	if (t64!=chunkid) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS incorrect chunkid (got:%"PRIu64" expected:%"PRIu64")",t64,chunkid);
		return -1;
	}
	if (t32!=writeid) {
		syslog(LOG_NOTICE,"writestatus; WRITE_STATUS incorrect writeid (got:%"PRIu32" expected:%"PRIu32")",t32,writeid);
		return -1;
	}
	return 0;
}

int cs_writeinit(int fd,uint8_t *chain,uint32_t chainsize,uint64_t chunkid,uint32_t version) {
	uint8_t *ptr,*ibuff;
	uint32_t psize;
	psize = 12+chainsize;
	ibuff = malloc(8+psize);
	if (ibuff==NULL) {
		syslog(LOG_NOTICE,"writestatus; WRITE_INIT out of memory");
		return -1;
	}
	ptr = ibuff;
	PUT32BIT(CUTOCS_WRITE,ptr);
	PUT32BIT(psize,ptr);
	PUT64BIT(chunkid,ptr);
	PUT32BIT(version,ptr);
	memcpy(ptr,chain,chainsize);
	psize+=8;
	if (tcptowrite(fd,ibuff,psize,CSMSECTIMEOUT)!=(int32_t)psize) {
		free(ibuff);
		syslog(LOG_NOTICE,"writestatus; WRITE_INIT tcpwrite error: %m");
		return -1;
	}
	free(ibuff);
	return cs_writestatus(fd,chunkid,0);	// wait for connect status
}

int cs_writeblock(int fd,uint64_t chunkid,uint32_t writeid,uint16_t blockno,uint16_t offset,uint32_t size,uint8_t *buff) {
	uint8_t *ptr,ibuff[32];
	uint32_t crc,psize;
	ptr = ibuff;
	PUT32BIT(CUTOCS_WRITE_DATA,ptr);
	psize = 24+size;
	PUT32BIT(psize,ptr);
	PUT64BIT(chunkid,ptr);
	PUT32BIT(writeid,ptr);
	PUT16BIT(blockno,ptr);
	PUT16BIT(offset,ptr);
	PUT32BIT(size,ptr);
	crc = crc32(0,buff,size);
	PUT32BIT(crc,ptr);
	if (tcptowrite(fd,ibuff,32,CSMSECTIMEOUT)!=32) {
		syslog(LOG_NOTICE,"writestatus; WRITE_DATA tcpwrite error: %m");
		return -1;
	}
	if (tcptowrite(fd,buff,size,CSMSECTIMEOUT)!=(int32_t)size) {
		syslog(LOG_NOTICE,"writestatus; WRITE_DATA tcpwrite error: %m");
		return -1;
	}
	return 0;
}
