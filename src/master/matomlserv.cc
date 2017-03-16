
/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o..

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
#include "master/matomlserv.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <set>

#include "common/cfg.h"
#include "common/crc.h"
#include "common/datapack.h"
#include "common/event_loop.h"
#include "common/lizardfs_version.h"
#include "common/loop_watchdog.h"
#include "common/massert.h"
#include "common/metadata.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "master/filesystem.h"
#include "master/personality.h"
#include "protocol/matoml.h"
#include "protocol/MFSCommunication.h"
#include "protocol/mltoma.h"

#define MaxPacketSize 1500000
#define OLD_CHANGES_BLOCK_SIZE 5000

// matomlserventry.mode
enum{KILL,HEADER,DATA};

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

typedef struct matomlserventry {
	uint8_t mode;
	int sock;
	int32_t pdescpos;
	uint32_t lastread,lastwrite;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;

	uint16_t timeout;

	char *servstrip;                // human readable version of servip
	uint32_t version;
	uint32_t servip;
	uint16_t servport;
	bool shadow;

	int metafd,chain1fd,chain2fd;

	struct matomlserventry *next;
} matomlserventry;

static matomlserventry *matomlservhead=NULL;
static int lsock;
static int32_t lsockpdescpos;
static bool gExiting = false;

/// Miminal period (in seconds) between two metadata save processes requested by shadow masters
static uint32_t gMinMetadataSaveRequestPeriod_s;

/// Timestamp of the last metadata save request
static uint32_t gLastMetadataSaveRequestTimestamp = 0;

typedef struct old_changes_entry {
	uint64_t version;
	uint32_t length;
	uint8_t *data;
} old_changes_entry;

typedef struct old_changes_block {
	old_changes_entry old_changes_block [OLD_CHANGES_BLOCK_SIZE];
	uint32_t entries;
	uint32_t mintimestamp;
	uint64_t minversion;
	struct old_changes_block *next;
} old_changes_block;

void matomlserv_createpacket(matomlserventry *eptr, std::vector<uint8_t> data);

/*! \brief Keep queue of Shadows interested in receiving information
 * regarding status of requested metadata dump.
 */
class ShadowQueue {
public:
	/*! \brief Add shadow connection handler to the queue.
	 *
	 * \param eptr - connection handler to be queued.
	 */
	void addRequest(matomlserventry* eptr) {
		shadowRequests_.insert(eptr);
	}

	/*! \brief Remove given connection handler from queue.
	 *
	 * \param eptr - connection handler to be removed from queue.
	 */
	void removeRequest(matomlserventry* eptr) {
		shadowRequests_.erase(eptr);
	}

	/*! \brief Handle all awaiting Shadows in queue.
	 *
	 * Broadcast metadata dump status and clear the queue.
	 *
	 * \param status - status of recently finished metadata dump.
	 */
	void handleRequests(uint8_t status) {
		for (matomlserventry* eptr : shadowRequests_) {
			matomlserv_createpacket(eptr, matoml::changelogApplyError::build(status));
		}
		shadowRequests_.clear();
	}
private:
	typedef std::set<matomlserventry*> ShadowRequests;
	ShadowRequests shadowRequests_;
} gShadowQueue;

/*! \brief Forward metadata dump status to Shadow queue.
 *
 * \param status - status to be forwarded.
 */
void matomlserv_broadcast_metadata_saved(uint8_t status) {
	gShadowQueue.handleRequests(status);
}

static old_changes_block *old_changes_head=NULL;
static old_changes_block *old_changes_current=NULL;

// from config
static char *ListenHost;
static char *ListenPort;
static uint16_t ChangelogSecondsToRemember;

void matomlserv_old_changes_free_block(old_changes_block *oc) {
	uint32_t i;
	for (i=0 ; i<oc->entries ; i++) {
		free(oc->old_changes_block[i].data);
	}
	free(oc);
}

void matomlserv_store_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize) {\
	old_changes_block *oc;
	old_changes_entry *oce;
	uint32_t ts;
	if (ChangelogSecondsToRemember==0) {
		while (old_changes_head) {
			oc = old_changes_head->next;
			matomlserv_old_changes_free_block(old_changes_head);
			old_changes_head = oc;
		}
		return;
	}
	if (old_changes_current==NULL || old_changes_head==NULL || old_changes_current->entries>=OLD_CHANGES_BLOCK_SIZE) {
		oc = (old_changes_block*) malloc(sizeof(old_changes_block));
		passert(oc);
		ts = eventloop_time();
		oc->entries = 0;
		oc->minversion = version;
		oc->mintimestamp = ts;
		oc->next = NULL;
		if (old_changes_current==NULL || old_changes_head==NULL) {
			old_changes_head = old_changes_current = oc;
		} else {
			old_changes_current->next = oc;
			old_changes_current = oc;
		}
		while (old_changes_head && old_changes_head->next && old_changes_head->next->mintimestamp+ChangelogSecondsToRemember<ts) {
			oc = old_changes_head->next;
			matomlserv_old_changes_free_block(old_changes_head);
			old_changes_head = oc;
		}
	}
	oc = old_changes_current;
	oce = oc->old_changes_block + oc->entries;
	oce->version = version;
	oce->length = logstrsize;
	oce->data = (uint8_t*) malloc(logstrsize);
	passert(oce->data);
	memcpy(oce->data,logstr,logstrsize);
	oc->entries++;
}

uint32_t matomlserv_mloglist_size(void) {
	matomlserventry *eptr;
	uint32_t i;
	i=0;
	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (!eptr->shadow && eptr->mode!=KILL) {
			i++;
		}
	}
	return i*(4+4);
}

void matomlserv_mloglist_data(uint8_t *ptr) {
	matomlserventry *eptr;
	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (!eptr->shadow && eptr->mode!=KILL) {
			put32bit(&ptr,eptr->version);
			put32bit(&ptr,eptr->servip);
		}
	}
}

std::vector<MetadataserverListEntry> matomlserv_shadows() {
	std::vector<MetadataserverListEntry> ret;
	for (matomlserventry* eptr = matomlservhead; eptr; eptr=eptr->next) {
		if (eptr->shadow) {
			ret.emplace_back(eptr->servip, eptr->servport, eptr->version);
		}
	}
	return ret;
}

uint32_t matomlserv_shadows_count() {
	uint32_t count = 0;
	for (matomlserventry* eptr = matomlservhead; eptr; eptr=eptr->next) {
		if (eptr->shadow) {
			count++;
		}
	}
	return count;
}

void matomlserv_status(void) {
	matomlserventry *eptr;
	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->mode==HEADER || eptr->mode==DATA) {
			return;
		}
	}
	syslog(LOG_WARNING,"no meta loggers connected !!!");
}

char* matomlserv_makestrip(uint32_t ip) {
	uint8_t *ptr,pt[4];
	uint32_t l,i;
	char *optr;
	ptr = pt;
	put32bit(&ptr,ip);
	l=0;
	for (i=0 ; i<4 ; i++) {
		if (pt[i]>=100) {
			l+=3;
		} else if (pt[i]>=10) {
			l+=2;
		} else {
			l+=1;
		}
	}
	l+=4;
	optr = (char*) malloc(l);
	passert(optr);
	snprintf(optr,l,"%" PRIu8 ".%" PRIu8 ".%" PRIu8 ".%" PRIu8,pt[0],pt[1],pt[2],pt[3]);
	optr[l-1]=0;
	return optr;
}

uint8_t* matomlserv_createpacket(matomlserventry *eptr,uint32_t type,uint32_t size) {
	packetstruct *outpacket;
	uint8_t *ptr;
	uint32_t psize;

	outpacket=(packetstruct*)malloc(sizeof(packetstruct));
	passert(outpacket);
	psize = size+8;
	outpacket->packet= (uint8_t*) malloc(psize);
	passert(outpacket->packet);
	outpacket->bytesleft = psize;
	ptr = outpacket->packet;
	put32bit(&ptr,type);
	put32bit(&ptr,size);
	outpacket->startptr = (uint8_t*)(outpacket->packet);
	outpacket->next = NULL;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
	return ptr;
}

void matomlserv_createpacket(matomlserventry *eptr, std::vector<uint8_t> data) {
	packetstruct *outpacket = (packetstruct*) malloc(sizeof(packetstruct));
	passert(outpacket);
	outpacket->packet = (uint8_t*) malloc(data.size());
	passert(outpacket->packet);
	memcpy(outpacket->packet, data.data(), data.size());
	outpacket->bytesleft = data.size();
	outpacket->startptr = outpacket->packet;
	outpacket->next = nullptr;
	*(eptr->outputtail) = outpacket;
	eptr->outputtail = &(outpacket->next);
}

void matomlserv_send_old_changes(matomlserventry *eptr,uint64_t version) {
	old_changes_block *oc;
	old_changes_entry *oce;
	uint8_t *data;
	uint8_t start=0;
	uint32_t i;
	if (old_changes_head==NULL) {
		// syslog(LOG_WARNING,"meta logger wants old changes, but storage is disabled");
		return;
	}
	if (old_changes_head->minversion>version) {
		syslog(LOG_WARNING,"meta logger wants changes since version: %" PRIu64 ", but minimal version in storage is: %" PRIu64,version,old_changes_head->minversion);
		// TODO(msulikowski) send a special message which will cause the shadow master to unload fs
	}
	for (oc=old_changes_head ; oc ; oc=oc->next) {
		if (oc->minversion>=version) {
			start=1;
		} else if (oc->minversion<=version && (oc->next==NULL || oc->next->minversion>version)) {
			start=1;
		}
		if (start) {
			for (i=0 ; i<oc->entries ; i++) {
				oce = oc->old_changes_block + i;
				if (version < oce->version) {
					data = matomlserv_createpacket(eptr,MATOML_METACHANGES_LOG,9+oce->length);
					put8bit(&data,0xFF);
					put64bit(&data,oce->version);
					memcpy(data,oce->data,oce->length);
				}
			}
		}
	}
}

void matomlserv_register(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	if (eptr->version>0) {
		syslog(LOG_WARNING,"got register message from registered metalogger !!!");
		eptr->mode=KILL;
		return;
	}
	if (length<1) {
		syslog(LOG_NOTICE,"MLTOMA_REGISTER - wrong size (%" PRIu32 ")",length);
		eptr->mode=KILL;
		return;
	} else {
		uint8_t rversion = get8bit(&data);
		if (rversion < 1 || rversion > 4) {
			syslog(LOG_NOTICE,"MLTOMA_REGISTER - wrong version (%" PRIu8 ")",rversion);
			eptr->mode=KILL;
			return;
		}
		static const uint32_t expected_length[] = {0, 7, 7+8, 7, 7+8};
		if (length != expected_length[rversion]) {
			syslog(LOG_NOTICE,"MLTOMA_REGISTER (ver %" PRIu8 ") - wrong size (%" PRIu32
					"/%" PRIu32 ")", rversion, length, expected_length[rversion]);
			eptr->mode=KILL;
			return;
		}
		eptr->version = get32bit(&data);
		eptr->timeout = get16bit(&data);
		eptr->shadow = (rversion == 3 || rversion == 4);
		if (eptr->shadow) {
		// supported shadow master servers register using LIZ_MLTOMA_REGISTER_SHADOW packet
			syslog(LOG_NOTICE,
					"MLTOMA_REGISTER (ver %" PRIu8 ") - rejected old shadow master (v%s) from %s",
					rversion, lizardfsVersionToString(eptr->version).c_str(), eptr->servstrip);
			eptr->mode=KILL;
			return;
		}
		if (rversion == 2 || rversion == 4) {
			uint64_t minversion = get64bit(&data);
			matomlserv_send_old_changes(eptr,minversion);
		}
		if (eptr->timeout<10) {
			syslog(LOG_NOTICE,"MLTOMA_REGISTER communication timeout too small (%" PRIu16 " seconds - should be at least 10 seconds)",eptr->timeout);
			if (eptr->timeout<3) {
				eptr->timeout=3;
			}
		}
	}
}

void matomlserv_register_shadow(matomlserventry *eptr, const uint8_t *data, uint32_t length) {
	uint32_t version, timeout_ms;
	uint64_t shadowMetadataVersion;
	mltoma::registerShadow::deserialize(data, length, version, timeout_ms, shadowMetadataVersion);
	eptr->timeout = timeout_ms / 1000;
	eptr->version = version;
	eptr->shadow = true;
	if (eptr->timeout < 10) {
		syslog(LOG_NOTICE,
				"MLTOMA_REGISTER_SHADOW communication timeout too small (%" PRIu32 " milliseconds)"
				" - should be at least 10 seconds; increasing to 10 seconds", timeout_ms);
		if (eptr->timeout < 10) {
			eptr->timeout = 10;
		}
	}
	if (eptr->version < LIZARDFS_VERSHEX) {
		syslog(LOG_NOTICE,
				"MLTOMA_REGISTER_SHADOW - rejected old client (v%s) from %s",
				lizardfsVersionToString(eptr->version).c_str(), eptr->servstrip);
		matomlserv_createpacket(eptr, matoml::registerShadow::build(uint8_t(LIZARDFS_ERROR_REGISTER)));
		return;
	}

	uint64_t myMedatataVersion = fs_getversion();
	uint64_t replyVersion;
	if (myMedatataVersion > shadowMetadataVersion
			&& old_changes_head != nullptr
			&& old_changes_head->minversion <= shadowMetadataVersion) {
		// Our version is newer than shadow's, but we can cheat a bit by sending old changes
		replyVersion = shadowMetadataVersion;
	} else {
		// We don't have the required changes in memory. Let's say what is our version of metadata
		// and shadow will have to download our metadata file
		replyVersion = myMedatataVersion;
	}

	matomlserv_createpacket(eptr, matoml::registerShadow::build(LIZARDFS_VERSHEX, replyVersion));
	matomlserv_send_old_changes(eptr, replyVersion - 1); // this function expects lastlogversion
}

void matomlserv_matoclport(matomlserventry *eptr, const uint8_t *data, uint32_t length) {
	mltoma::matoclport::deserialize(data, length, eptr->servport);
}

void matomlserv_download_start(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	if (length!=1) {
		syslog(LOG_NOTICE,"MLTOMA_DOWNLOAD_START - wrong size (%" PRIu32 "/1)",length);
		eptr->mode=KILL;
		return;
	}
	if (gExiting) {
		syslog(LOG_NOTICE,"MLTOMA_DOWNLOAD_START - ignoring in the exit phase");
		return;
	}
	uint8_t filenum = get8bit(&data);

	if ((filenum == DOWNLOAD_METADATA_MFS) || (filenum == DOWNLOAD_SESSIONS_MFS)) {
		if (eptr->metafd>=0) {
			close(eptr->metafd);
			eptr->metafd=-1;
		}
		if (eptr->chain1fd>=0) {
			close(eptr->chain1fd);
			eptr->chain1fd=-1;
		}
		if (eptr->chain2fd>=0) {
			close(eptr->chain2fd);
			eptr->chain2fd=-1;
		}
	}
	if (filenum == DOWNLOAD_METADATA_MFS) {
		eptr->metafd = open(kMetadataFilename, O_RDONLY);
		eptr->chain1fd = open(kChangelogFilename, O_RDONLY);
		eptr->chain2fd = open((std::string(kChangelogFilename) + ".1").c_str(), O_RDONLY);
	} else if (filenum == DOWNLOAD_SESSIONS_MFS) {
		eptr->metafd = open(kSessionsFilename, O_RDONLY);
	} else if (filenum == DOWNLOAD_CHANGELOG_MFS) {
		if (eptr->metafd>=0) {
			close(eptr->metafd);
		}
		eptr->metafd = eptr->chain1fd;
		eptr->chain1fd = -1;
	} else if (filenum == DOWNLOAD_CHANGELOG_MFS_1) {
		if (eptr->metafd>=0) {
			close(eptr->metafd);
		}
		eptr->metafd = eptr->chain2fd;
		eptr->chain2fd = -1;
	} else {
		eptr->mode=KILL;
		return;
	}
	uint8_t *ptr;
	if (eptr->metafd<0) {
		if (filenum==11 || filenum==12) {
			ptr = matomlserv_createpacket(eptr,MATOML_DOWNLOAD_START,8);
			put64bit(&ptr,0);
			return;
		} else {
			ptr = matomlserv_createpacket(eptr,MATOML_DOWNLOAD_START,1);
			put8bit(&ptr,0xff);     // error
			return;
		}
	}
	uint64_t size = lseek(eptr->metafd,0,SEEK_END);
	ptr = matomlserv_createpacket(eptr,MATOML_DOWNLOAD_START,8);
	put64bit(&ptr,size);    // ok
}

void matomlserv_download_data(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	uint8_t *ptr;
	uint64_t offset;
	uint32_t leng;
	uint32_t crc;
	ssize_t ret;

	if (length!=12) {
		syslog(LOG_NOTICE,"MLTOMA_DOWNLOAD_DATA - wrong size (%" PRIu32 "/12)",length);
		eptr->mode=KILL;
		return;
	}
	if (gExiting) {
		syslog(LOG_NOTICE,"MLTOMA_DOWNLOAD_DATA - ignoring in the exit phase");
		return;
	}
	if (eptr->metafd<0) {
		syslog(LOG_NOTICE,"MLTOMA_DOWNLOAD_DATA - file not opened");
		eptr->mode=KILL;
		return;
	}
	offset = get64bit(&data);
	leng = get32bit(&data);
	ptr = matomlserv_createpacket(eptr,MATOML_DOWNLOAD_DATA,16+leng);
	put64bit(&ptr,offset);
	put32bit(&ptr,leng);
	ret = pread(eptr->metafd,ptr+4,leng,offset);
	if (ret!=(ssize_t)leng) {
		lzfs_silent_errlog(LOG_NOTICE,"error reading metafile");
		eptr->mode=KILL;
		return;
	}
	crc = mycrc32(0,ptr+4,leng);
	put32bit(&ptr,crc);
}

void matomlserv_download_end(matomlserventry *eptr,const uint8_t *data,uint32_t length) {
	(void)data;
	if (length!=0) {
		syslog(LOG_NOTICE,"MLTOMA_DOWNLOAD_END - wrong size (%" PRIu32 "/0)",length);
		eptr->mode=KILL;
		return;
	}
	if (eptr->metafd>=0) {
		close(eptr->metafd);
		eptr->metafd=-1;
	}
}

void matomlserv_changelog_apply_error(matomlserventry *eptr, const uint8_t *data, uint32_t length) {
	uint8_t recvStatus;
	mltoma::changelogApplyError::deserialize(data, length, recvStatus);
	if (gExiting) {
		syslog(LOG_NOTICE,"LIZ_MLTOMA_CHANGELOG_APPLY_ERROR - ignoring in the exit phase");
		return;
	}

	int32_t secondsSinceLastRequest = eventloop_time() - gLastMetadataSaveRequestTimestamp;
	if (secondsSinceLastRequest >= int32_t(gMinMetadataSaveRequestPeriod_s)) {
		gLastMetadataSaveRequestTimestamp = eventloop_time();
		DEBUG_LOG("master.mltoma_changelog_apply_error") << "do";
		syslog(LOG_INFO,
				"LIZ_MLTOMA_CHANGELOG_APPLY_ERROR, status: %s - storing metadata",
				mfsstrerr(recvStatus));
		gShadowQueue.addRequest(eptr);
		fs_storeall(MetadataDumper::kBackgroundDump);
		if (recvStatus == LIZARDFS_ERROR_BADMETADATACHECKSUM) {
			fs_start_checksum_recalculation();
		}
	} else {
		DEBUG_LOG("master.mltoma_changelog_apply_error") << "delay";
		syslog(LOG_INFO,
				"LIZ_MLTOMA_CHANGELOG_APPLY_ERROR, status: %s - "
				"refusing to store metadata because only %" PRIi32 " seconds elapsed since the "
				"previous request and METADATA_SAVE_REQUEST_MIN_PERIOD=%" PRIu32,
				mfsstrerr(recvStatus), secondsSinceLastRequest, gMinMetadataSaveRequestPeriod_s);
		matomlserv_createpacket(eptr, matoml::changelogApplyError::build(LIZARDFS_ERROR_DELAYED));
	}
}

void matomlserv_broadcast_logstring(uint64_t version,uint8_t *logstr,uint32_t logstrsize) {
	matomlserventry *eptr;
	uint8_t *data;

	matomlserv_store_logstring(version,logstr,logstrsize);

	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->version>0) {
			data = matomlserv_createpacket(eptr,MATOML_METACHANGES_LOG,9+logstrsize);
			put8bit(&data,0xFF);
			put64bit(&data,version);
			memcpy(data,logstr,logstrsize);
		}
	}
}

void matomlserv_broadcast_logrotate() {
	matomlserventry *eptr;
	uint8_t *data;

	for (eptr = matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->version>0) {
			data = matomlserv_createpacket(eptr,MATOML_METACHANGES_LOG,1);
			put8bit(&data, FORCE_LOG_ROTATE);
		}
	}
}

void matomlserv_beforeclose(matomlserventry *eptr) {
	if (eptr->metafd>=0) {
		close(eptr->metafd);
		eptr->metafd=-1;
	}
	if (eptr->chain1fd>=0) {
		close(eptr->chain1fd);
		eptr->chain1fd=-1;
	}
	if (eptr->chain2fd>=0) {
		close(eptr->chain2fd);
		eptr->chain2fd=-1;
	}
	gShadowQueue.removeRequest(eptr);
}

void matomlserv_gotpacket(matomlserventry *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	try {
		switch (type) {
			case ANTOAN_NOP:
				break;
			case ANTOAN_UNKNOWN_COMMAND: // for future use
				break;
			case ANTOAN_BAD_COMMAND_SIZE: // for future use
				break;
			case MLTOMA_REGISTER:
				matomlserv_register(eptr,data,length);
				break;
			case LIZ_MLTOMA_REGISTER_SHADOW:
				matomlserv_register_shadow(eptr,data,length);
				break;
			case MLTOMA_DOWNLOAD_START:
				matomlserv_download_start(eptr,data,length);
				break;
			case MLTOMA_DOWNLOAD_DATA:
				matomlserv_download_data(eptr,data,length);
				break;
			case MLTOMA_DOWNLOAD_END:
				matomlserv_download_end(eptr,data,length);
				break;
			case LIZ_MLTOMA_CHANGELOG_APPLY_ERROR:
				matomlserv_changelog_apply_error(eptr, data, length);
				break;
			case LIZ_MLTOMA_CLTOMA_PORT:
				matomlserv_matoclport(eptr, data, length);
				break;
			default:
				syslog(LOG_NOTICE,"master <-> metaloggers module: got unknown message (type:%" PRIu32 ")",type);
				eptr->mode=KILL;
		}
	} catch (IncorrectDeserializationException& ex) {
		syslog(LOG_NOTICE, "Packet 0x%" PRIX32 " - can't deserialize: %s", type, ex.what());
		eptr->mode = KILL;
	}
}

void matomlserv_term(void) {
	matomlserventry *eptr,*eaptr;
	packetstruct *pptr,*paptr;
	syslog(LOG_INFO,"master <-> metaloggers module: closing %s:%s",ListenHost,ListenPort);
	tcpclose(lsock);

	eptr = matomlservhead;
	while (eptr) {
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
		}
		if (eptr->servstrip) {
			free(eptr->servstrip);
		}
		pptr = eptr->outputhead;
		while (pptr) {
			if (pptr->packet) {
				free(pptr->packet);
			}
			paptr = pptr;
			pptr = pptr->next;
			free(paptr);
		}
		eaptr = eptr;
		eptr = eptr->next;
		gShadowQueue.removeRequest(eaptr);
		free(eaptr);
	}
	matomlservhead=NULL;

	free(ListenHost);
	free(ListenPort);
}

void matomlserv_read(matomlserventry *eptr) {
	SignalLoopWatchdog watchdog;
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;

	watchdog.start();
	while (eptr->mode != KILL) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
			syslog(LOG_NOTICE,"connection with ML(%s) has been closed by peer",eptr->servstrip);
			eptr->mode = KILL;
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE,"read from ML(%s) error",eptr->servstrip);
				eptr->mode = KILL;
			}
			return;
		}
		eptr->inputpacket.startptr+=i;
		eptr->inputpacket.bytesleft-=i;

		if (eptr->inputpacket.bytesleft>0) {
			return;
		}

		if (eptr->mode==HEADER) {
			ptr = eptr->hdrbuff+4;
			size = get32bit(&ptr);

			if (size>0) {
				if (size>MaxPacketSize) {
					syslog(LOG_WARNING,"ML(%s) packet too long (%" PRIu32 "/%u)",eptr->servstrip,size,MaxPacketSize);
					eptr->mode = KILL;
					return;
				}
				eptr->inputpacket.packet = (uint8_t*) malloc(size);
				passert(eptr->inputpacket.packet);
				eptr->inputpacket.bytesleft = size;
				eptr->inputpacket.startptr = eptr->inputpacket.packet;
				eptr->mode = DATA;
				continue;
			}
			eptr->mode = DATA;
		}

		if (eptr->mode==DATA) {
			ptr = eptr->hdrbuff;
			type = get32bit(&ptr);
			size = get32bit(&ptr);

			eptr->mode=HEADER;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;

			matomlserv_gotpacket(eptr,type,eptr->inputpacket.packet,size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet=NULL;
			break;
		}

		if (watchdog.expired()) {
			break;
		}
	}
}

void matomlserv_write(matomlserventry *eptr) {
	SignalLoopWatchdog watchdog;
	packetstruct *pack;
	int32_t i;

	watchdog.start();
	for (;;) {
		pack = eptr->outputhead;
		if (pack==NULL) {
			return;
		}
		i=write(eptr->sock,pack->startptr,pack->bytesleft);
		if (i<0) {
			if (errno!=EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE,"write to ML(%s) error",eptr->servstrip);
				eptr->mode = KILL;
			}
			return;
		}
		pack->startptr+=i;
		pack->bytesleft-=i;
		if (pack->bytesleft>0) {
			return;
		}
		free(pack->packet);
		eptr->outputhead = pack->next;
		if (eptr->outputhead==NULL) {
			eptr->outputtail = &(eptr->outputhead);
		}
		free(pack);

		if (watchdog.expired()) {
			break;
		}
	}
}

void matomlserv_desc(std::vector<pollfd> &pdesc) {
	matomlserventry *eptr;
	if (!gExiting) {
		pdesc.push_back({lsock,POLLIN,0});
		lsockpdescpos = pdesc.size() - 1;
	} else {
		lsockpdescpos = -1;
	}
	for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
		pdesc.push_back({eptr->sock,POLLIN,0});
		eptr->pdescpos = pdesc.size() - 1;
		if (eptr->outputhead!=NULL) {
			pdesc.back().events |= POLLOUT;
		}
	}
}

void matomlserv_serve(const std::vector<pollfd> &pdesc) {
	uint32_t now=eventloop_time();
	matomlserventry *eptr,**kptr;
	packetstruct *pptr,*paptr;
	int ns;

	if (lsockpdescpos>=0 && (pdesc[lsockpdescpos].revents & POLLIN)) {
		ns=tcpaccept(lsock);
		if (ns<0) {
			lzfs_silent_errlog(LOG_NOTICE,"master<->ML socket: accept error");
		} else if (metadataserver::isMaster()) {
			tcpnonblock(ns);
			tcpnodelay(ns);
			eptr = (matomlserventry*) malloc(sizeof(matomlserventry));
			passert(eptr);
			eptr->next = matomlservhead;
			matomlservhead = eptr;
			eptr->sock = ns;
			eptr->pdescpos = -1;
			eptr->mode = HEADER;
			eptr->lastread = now;
			eptr->lastwrite = now;
			eptr->inputpacket.next = NULL;
			eptr->inputpacket.bytesleft = 8;
			eptr->inputpacket.startptr = eptr->hdrbuff;
			eptr->inputpacket.packet = NULL;
			eptr->outputhead = NULL;
			eptr->outputtail = &(eptr->outputhead);
			eptr->timeout = 10;
			eptr->servport = 0;// For shadow masters this will be changed to their MATOCL_SERV_PORT
			eptr->shadow = false;

			tcpgetpeer(eptr->sock,&(eptr->servip),NULL);
			eptr->servstrip = matomlserv_makestrip(eptr->servip);
			eptr->version=0;
			eptr->metafd=-1;
			eptr->chain1fd=-1;
			eptr->chain2fd=-1;
		} else {
			tcpclose(ns);
		}
	}
	for (eptr=matomlservhead ; eptr ; eptr=eptr->next) {
		if (eptr->pdescpos>=0) {
			if (pdesc[eptr->pdescpos].revents & (POLLERR|POLLHUP)) {
				eptr->mode = KILL;
			}
			if ((pdesc[eptr->pdescpos].revents & POLLIN) && eptr->mode!=KILL) {
				eptr->lastread = now;
				matomlserv_read(eptr);
			}
			if ((pdesc[eptr->pdescpos].revents & POLLOUT) && eptr->mode!=KILL) {
				eptr->lastwrite = now;
				matomlserv_write(eptr);
			}
		}
		if ((uint32_t)(eptr->lastread+eptr->timeout)<(uint32_t)now) {
			eptr->mode = KILL;
		}
		if ((uint32_t)(eptr->lastwrite+(eptr->timeout/3))<(uint32_t)now
				&& eptr->outputhead==NULL
				&& !gExiting) {
			matomlserv_createpacket(eptr,ANTOAN_NOP,0);
		}
	}
	kptr = &matomlservhead;
	while ((eptr=*kptr)) {
		if (eptr->mode == KILL) {
			matomlserv_beforeclose(eptr);
			tcpclose(eptr->sock);
			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			pptr = eptr->outputhead;
			while (pptr) {
				if (pptr->packet) {
					free(pptr->packet);
				}
				paptr = pptr;
				pptr = pptr->next;
				free(paptr);
			}
			if (eptr->servstrip) {
				free(eptr->servstrip);
			}
			*kptr = eptr->next;
			free(eptr);
		} else {
			kptr = &(eptr->next);
		}
	}
}

void matomlserv_wantexit(void) {
	gExiting = true;
	for (matomlserventry *eptr = matomlservhead; eptr != nullptr; eptr = eptr->next) {
		matomlserv_createpacket(eptr, matoml::endSession::build());
	}
	// Now we won't create any new packets, but we will wait for all existing packets to be
	// transmitted to shadow masters and metaloggers
}

int matomlserv_canexit(void) {
	// Exit when all connections are closed
	return (matomlservhead == nullptr);
}

void matomlserv_become_master() {
	eventloop_timeregister(TIMEMODE_SKIP_LATE,3600,0,matomlserv_status);
	return;
}

/// Read values from the config file into global variables.
/// Used on init and reload.
void matomlserv_read_config_file() {
	gMinMetadataSaveRequestPeriod_s = cfg_getuint32("METADATA_SAVE_REQUEST_MIN_PERIOD", 1800);
}

void matomlserv_reload(void) {
	char *oldListenHost,*oldListenPort;
	int newlsock;

	matomlserv_read_config_file();

	oldListenHost = ListenHost;
	oldListenPort = ListenPort;
	ListenHost = cfg_getstr("MATOML_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOML_LISTEN_PORT","9419");
	if (strcmp(oldListenHost,ListenHost)==0 && strcmp(oldListenPort,ListenPort)==0) {
		free(oldListenHost);
		free(oldListenPort);
		lzfs_pretty_syslog(LOG_NOTICE,"master <-> metaloggers module: socket address hasn't changed (%s:%s)",ListenHost,ListenPort);
		return;
	}

	newlsock = tcpsocket();
	if (newlsock<0) {
		lzfs_pretty_errlog(LOG_WARNING,"master <-> metaloggers module: socket address has changed, but can't create new socket");
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		return;
	}
	tcpnonblock(newlsock);
	tcpnodelay(newlsock);
	tcpreuseaddr(newlsock);
	if (tcpsetacceptfilter(newlsock)<0 && errno!=ENOTSUP) {
		lzfs_silent_errlog(LOG_NOTICE,"master <-> metaloggers module: can't set accept filter");
	}
	if (tcpstrlisten(newlsock,ListenHost,ListenPort,100)<0) {
		lzfs_pretty_errlog(LOG_ERR,"master <-> metaloggers module: socket address has changed, but can't listen on socket (%s:%s)",ListenHost,ListenPort);
		free(ListenHost);
		free(ListenPort);
		ListenHost = oldListenHost;
		ListenPort = oldListenPort;
		tcpclose(newlsock);
		return;
	}
	lzfs_pretty_syslog(LOG_NOTICE,"master <-> metaloggers module: socket address has changed, now listen on %s:%s",ListenHost,ListenPort);
	free(oldListenHost);
	free(oldListenPort);
	tcpclose(lsock);
	lsock = newlsock;

	ChangelogSecondsToRemember = cfg_getuint16("MATOML_LOG_PRESERVE_SECONDS",600);
	if (ChangelogSecondsToRemember>3600) {
		syslog(LOG_WARNING,"Number of seconds of change logs to be preserved in master is too big (%" PRIu16 ") - decreasing to 3600 seconds",ChangelogSecondsToRemember);
		ChangelogSecondsToRemember=3600;
	}
}

int matomlserv_init(void) {
	matomlserv_read_config_file();
	ListenHost = cfg_getstr("MATOML_LISTEN_HOST","*");
	ListenPort = cfg_getstr("MATOML_LISTEN_PORT","9419");

	lsock = tcpsocket();
	if (lsock<0) {
		lzfs_pretty_errlog(LOG_ERR,"master <-> metaloggers module: can't create socket");
		return -1;
	}
	tcpnonblock(lsock);
	tcpnodelay(lsock);
	tcpreuseaddr(lsock);
	if (tcpsetacceptfilter(lsock)<0 && errno!=ENOTSUP) {
		lzfs_silent_errlog(LOG_NOTICE,"master <-> metaloggers module: can't set accept filter");
	}

	if (tcpstrlisten(lsock,ListenHost,ListenPort,100)<0) {
		lzfs_pretty_errlog(LOG_ERR,"master <-> metaloggers module: can't listen on %s:%s",ListenHost,ListenPort);
		return -1;
	}
	lzfs_pretty_syslog(LOG_NOTICE,"master <-> metaloggers module: listen on %s:%s",ListenHost,ListenPort);

	matomlservhead = NULL;
	ChangelogSecondsToRemember = cfg_getuint16("MATOML_LOG_PRESERVE_SECONDS",600);
	if (ChangelogSecondsToRemember>3600) {
		syslog(LOG_WARNING,"Number of seconds of change logs to be preserved in master is too big (%" PRIu16 ") - decreasing to 3600 seconds",ChangelogSecondsToRemember);
		ChangelogSecondsToRemember=3600;
	}
	eventloop_wantexitregister(matomlserv_wantexit);
	eventloop_canexitregister(matomlserv_canexit);
	eventloop_reloadregister(matomlserv_reload);
	metadataserver::registerFunctionCalledOnPromotion(matomlserv_become_master);
	eventloop_destructregister(matomlserv_term);
	eventloop_pollregister(matomlserv_desc,matomlserv_serve);
	if (metadataserver::isMaster()) {
		matomlserv_become_master();
	}
	return 0;
}

