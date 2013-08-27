/*
 Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA.

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

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <syslog.h>

#include "mfscommon/MFSCommunication.h"
#include "mfscommon/sockets.h"
#include "mfscommon/datapack.h"
#include "mfscommon/strerr.h"
#include "mfscommon/mfsstrerr.h"
#include "mfscommon/crc.h"

#include "devtools/TracePrinter.h"

#define CSMSECTIMEOUT 5000

int cs_read(int fd, uint64_t chunkid, uint32_t version, uint32_t offset,
		uint32_t size, uint8_t *buff) {
	TRACETHIS4(chunkid, version, offset, size);
	uint8_t *wptr, ibuff[28];
	const uint8_t *rptr;

	wptr = ibuff;
	put32bit(&wptr, CLTOCS_READ);
	put32bit(&wptr, 20);
	put64bit(&wptr, chunkid);
	put32bit(&wptr, version);
	put32bit(&wptr, offset);
	put32bit(&wptr, size);
	if (tcptowrite(fd, ibuff, 28, CSMSECTIMEOUT) != 28) {
		syslog(LOG_NOTICE, "cs read; tcpwrite error: %s", strerr(errno));
		return -1;
	}
	for (;;) {
		uint32_t cmd, l;
		uint64_t t64;
		uint32_t readoffset;
		uint32_t readsize, readcrc;
		if (tcptoread(fd, ibuff, 8, CSMSECTIMEOUT) != 8) {
			syslog(LOG_NOTICE, "cs read; tcpread error: %s", strerr(errno));
			return -1;
		}
		rptr = ibuff;
		cmd = get32bit(&rptr);
		l = get32bit(&rptr);
		if (cmd == CSTOCL_READ_STATUS) {
			if (l != 9) {
				syslog(LOG_NOTICE,"cs read; READ_STATUS incorrect message size (%" PRIu32 "/9)",l);
				return -1;
			}
			if (tcptoread(fd, ibuff, 9, CSMSECTIMEOUT) != 9) {
				syslog(LOG_NOTICE, "cs read; READ_STATUS tcpread error: %s",
						strerr(errno));
				return -1;
			}
			rptr = ibuff;
			t64 = get64bit(&rptr);
			if (*rptr != 0) {
				syslog(LOG_NOTICE, "cs read; READ_STATUS status: %s",
						mfsstrerr(*rptr));
				return -1;
			}
			if (t64 != chunkid) {
				syslog(LOG_NOTICE,"cs read; READ_STATUS incorrect chunkid (got:%" PRIu64 " expected:%" PRIu64 ")",t64,chunkid);
				return -1;
			}
			if (size != 0) {
				syslog(LOG_NOTICE,"cs read; READ_STATUS incorrect data size (left: %" PRIu32 ")",size);
				return -1;
			}
			return 0;
		} else if (cmd == CSTOCL_READ_DATA) {
			if (l < 20) {
				syslog(LOG_NOTICE,"cs read; READ_DATA incorrect message size (%" PRIu32 "/>=20)",l);
				return -1;
			}
			if (tcptoread(fd, ibuff, 20, CSMSECTIMEOUT) != 20) {
				syslog(LOG_NOTICE, "readblock; READ_DATA tcpread error: %s",
						strerr(errno));
				return -1;
			}
			rptr = ibuff;
			t64 = get64bit(&rptr);
			if (t64 != chunkid) {
				syslog(LOG_NOTICE,"cs read; READ_DATA incorrect chunkid (got:%" PRIu64 " expected:%" PRIu64 ")",t64,chunkid);
				return -1;
			}

			readoffset = get32bit(&rptr);
			readsize = get32bit(&rptr);
			readcrc = get32bit(&rptr);
			if (l != 20 + readsize) {
				syslog(LOG_NOTICE,"cs read; READ_DATA incorrect message size (%" PRIu32 ")",20+readsize);
				return -1;
			}
			if (readsize == 0) {
				syslog(LOG_NOTICE, "cs read; READ_DATA empty read");
				return -1;
			}
			if (readoffset != offset) {
				syslog(LOG_NOTICE,"cs read; READ_DATA incorrect offset (got:%" PRIu16 " expected:%" PRIu32 ")", readoffset,  offset);
				return -1;
			}
			if (readsize > size) {
				syslog(LOG_NOTICE,"cs read; READ_DATA incorrect size (got:%" PRIu32 " requested:%" PRIu32 ")", readsize, size);
				return -1;
			}
			if (tcptoread(fd, buff, readsize, CSMSECTIMEOUT)
					!= (int32_t) readsize) {
				syslog(LOG_NOTICE, "cs read; READ_DATA tcpread error: %s",
						strerr(errno));
				return -1;
			}
			if (mycrc32(0, buff, readsize) != readcrc) {
				syslog(LOG_NOTICE,"cs read; READ_DATA crc checksum error");
				return -1;
			}

			size -= readsize;
			buff += readsize;
			offset += readsize;
		} else if (cmd == ANTOAN_NOP) {
			if (l != 0) {
				syslog(LOG_NOTICE,"cs read; NOP incorrect message size (%" PRIu32 "/0)",l);
				return -1;
			}
		} else if (cmd == ANTOAN_UNKNOWN_COMMAND
				|| cmd == ANTOAN_BAD_COMMAND_SIZE) {
			syslog(LOG_NOTICE,
					"cs read; got UNKNOWN_COMMAND/BAD_COMMAND_SIZE !!!");
			return -1;
		} else {
			syslog(LOG_NOTICE, "cs read; unknown message");
			return -1;
		}
	}
	return 0;
}
