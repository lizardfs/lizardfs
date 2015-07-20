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

#include "common/platform.h"
#include "mount/cscomm.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "common/crc.h"
#include "common/datapack.h"
#include "common/MFSCommunication.h"
#include "common/mfserr.h"
#include "common/slogger.h"
#include "common/sockets.h"

#define CSMSECTIMEOUT 5000

int cs_readblock(int fd,uint64_t chunkid,uint32_t version,uint32_t offset,uint32_t size,uint8_t *buff) {
	uint8_t *wptr,ibuff[28];
	const uint8_t *rptr;

	wptr = ibuff;
	put32bit(&wptr,CLTOCS_READ);
	put32bit(&wptr,20);
	put64bit(&wptr,chunkid);
	put32bit(&wptr,version);
	put32bit(&wptr,offset);
	put32bit(&wptr,size);
	if (tcptowrite(fd,ibuff,28,CSMSECTIMEOUT)!=28) {
		lzfs_pretty_syslog(LOG_NOTICE,
				"readblock; tcpwrite error: %s",
				strerr(tcpgetlasterror()));
		return -1;
	}
	for (;;) {
		uint32_t cmd,l;
		uint64_t t64;
		uint16_t blockno,blockoffset;
		uint32_t breq,blocksize,blockcrc;
		if (tcptoread(fd,ibuff,8,CSMSECTIMEOUT)!=8) {
			lzfs_pretty_syslog(LOG_NOTICE,
					"readblock; tcpread error: %s",
					strerr(tcpgetlasterror()));
			return -1;
		}
		rptr = ibuff;
		cmd = get32bit(&rptr);
		l = get32bit(&rptr);
		if (cmd==CSTOCL_READ_STATUS) {
			if (l!=9) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_STATUS incorrect message size (%" PRIu32 "/9)",l);
				return -1;
			}
			if (tcptoread(fd,ibuff,9,CSMSECTIMEOUT)!=9) {
				lzfs_pretty_syslog(LOG_NOTICE,
						"readblock; READ_STATUS tcpread error: %s",
						strerr(tcpgetlasterror()));
				return -1;
			}
			rptr = ibuff;
			t64 = get64bit(&rptr);
			if (*rptr!=0) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_STATUS status: %s",mfsstrerr(*rptr));
				return -1;
			}
			if (t64!=chunkid) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_STATUS incorrect chunkid (got:%" PRIu64 " expected:%" PRIu64 ")",t64,chunkid);
				return -1;
			}
			if (size!=0) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_STATUS incorrect data size (left: %" PRIu32 ")",size);
				return -1;
			}
			return 0;
		} else if (cmd==CSTOCL_READ_DATA) {
			if (l<20) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_DATA incorrect message size (%" PRIu32 "/>=20)",l);
				return -1;
			}
			if (tcptoread(fd,ibuff,20,CSMSECTIMEOUT)!=20) {
				lzfs_pretty_syslog(LOG_NOTICE,
						"readblock; READ_DATA tcpread error: %s",
						strerr(tcpgetlasterror()));
				return -1;
			}
			rptr = ibuff;
			t64 = get64bit(&rptr);
			if (t64!=chunkid) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_DATA incorrect chunkid (got:%" PRIu64 " expected:%" PRIu64 ")",t64,chunkid);
				return -1;
			}
			blockno = get16bit(&rptr);
			blockoffset = get16bit(&rptr);
			blocksize = get32bit(&rptr);
			blockcrc = get32bit(&rptr);
			if (l!=20+blocksize) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_DATA incorrect message size (%" PRIu32 "/%" PRIu32 ")",l,20+blocksize);
				return -1;
			}
			if (blocksize==0) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_DATA empty block");
				return -1;
			}
			if (blockno!=(offset>>MFSBLOCKBITS)) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_DATA incorrect block number (got:%" PRIu16 " expected:%" PRIu32 ")",blockno,(offset>>MFSBLOCKBITS));
				return -1;
			}
			if (blockoffset!=(offset&MFSBLOCKMASK)) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_DATA incorrect block offset (got:%" PRIu16 " expected:%" PRIu32 ")",blockoffset,(offset&MFSBLOCKMASK));
				return -1;
			}
			breq = MFSBLOCKSIZE - (uint32_t)blockoffset;
			if (size<breq) {
				breq=size;
			}
			if (blocksize!=breq) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_DATA incorrect block size (got:%" PRIu32 " expected:%" PRIu32 ")",blocksize,breq);
				return -1;
			}
			if (tcptoread(fd,buff,blocksize,CSMSECTIMEOUT)!=(int32_t)blocksize) {
				lzfs_pretty_syslog(LOG_NOTICE,
						"readblock; READ_DATA tcpread error: %s",
						strerr(tcpgetlasterror()));
				return -1;
			}
			if (blockcrc!=mycrc32(0,buff,blocksize)) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; READ_DATA crc checksum error");
				return -1;
			}
			offset+=blocksize;
			size-=blocksize;
			buff+=blocksize;
		} else if (cmd==ANTOAN_NOP) {
			if (l!=0) {
				lzfs_pretty_syslog(LOG_NOTICE,"readblock; NOP incorrect message size (%" PRIu32 "/0)",l);
				return -1;
			}
		} else if (cmd==ANTOAN_UNKNOWN_COMMAND || cmd==ANTOAN_BAD_COMMAND_SIZE) {
			lzfs_pretty_syslog(LOG_NOTICE,"readblock; got UNKNOWN_COMMAND/BAD_COMMAND_SIZE !!!");
			return -1;
		} else {
			lzfs_pretty_syslog(LOG_NOTICE,"readblock; unknown message");
			return -1;
		}
	}
	return 0;
}
