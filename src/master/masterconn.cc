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
#include "master/masterconn.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>
#include <string>

#include "common/cfg.h"
#include "common/crc.h"
#include "common/cwrap.h"
#include "common/datapack.h"
#include "common/event_loop.h"
#include "common/lizardfs_version.h"
#include "common/loop_watchdog.h"
#include "common/massert.h"
#include "common/metadata.h"
#include "common/rotate_files.h"
#include "common/slogger.h"
#include "common/sockets.h"
#include "common/time_utils.h"
#include "master/changelog.h"
#include "protocol/matoml.h"
#include "protocol/MFSCommunication.h"
#include "protocol/mltoma.h"

#ifndef METALOGGER
#include "master/filesystem.h"
#include "master/personality.h"
#include "master/restore.h"
#endif /* #ifndef METALOGGER */

#define MaxPacketSize 1500000

#define META_DL_BLOCK 1000000

// mode
enum {FREE,CONNECTING,HEADER,DATA,KILL};

enum class MasterConnectionState {
	kNone,
	kSynchronized,
	kDownloading,
	kDumpRequestPending,
	kLimbo /*!< Got response from master regarding its inability to dump metadata. */
};

typedef struct packetstruct {
	struct packetstruct *next;
	uint8_t *startptr;
	uint32_t bytesleft;
	uint8_t *packet;
} packetstruct;

struct masterconn {
	int mode;
	int sock;
	uint32_t version; // version of the master server; known by shadow masters after registration
	int32_t pdescpos;
	uint32_t lastread,lastwrite;
	uint8_t hdrbuff[8];
	packetstruct inputpacket;
	packetstruct *outputhead,**outputtail;
	uint32_t bindip;
	uint32_t masterip;
	uint16_t masterport;
	uint8_t masteraddrvalid;

	uint8_t downloadretrycnt;
	uint8_t downloading;
	int metafd;     // using standard unix I/O because this is binary file
	uint64_t filesize;
	uint64_t dloffset;
	uint64_t dlstartuts;
	void* sessionsdownloadinit_handle;
	void* metachanges_flush_handle;
	MasterConnectionState state;
	uint8_t error_status;
	Timeout changelog_apply_error_packet_time;
	masterconn()
			: mode(),
			  sock(),
			  version(),
			  pdescpos(),
			  lastread(),
			  lastwrite(),
			  hdrbuff(),
			  inputpacket(),
			  outputhead(),
			  outputtail(),
			  bindip(),
			  masterip(),
			  masterport(),
			  masteraddrvalid(),
			  downloadretrycnt(),
			  downloading(),
			  metafd(),
			  filesize(),
			  dloffset(),
			  dlstartuts(),
			  sessionsdownloadinit_handle(),
			  metachanges_flush_handle(),
			  state(),
			  error_status(),
			  changelog_apply_error_packet_time(std::chrono::seconds(10)) {
	}
};

static masterconn *masterconnsingleton=NULL;

// from config
static uint32_t BackMetaCopies;
static std::string MasterHost;
static std::string MasterPort;
static std::string BindHost;
static uint32_t Timeout;
static void* reconnect_hook;
#ifdef METALOGGER
static void* download_hook;
#endif /* #ifndef METALOGGER */
static uint64_t lastlogversion=0;

static uint32_t stats_bytesout=0;
static uint32_t stats_bytesin=0;

void masterconn_stats(uint32_t *bin,uint32_t *bout) {
	*bin = stats_bytesin;
	*bout = stats_bytesout;
	stats_bytesin = 0;
	stats_bytesout = 0;
}

#ifdef METALOGGER
void masterconn_findlastlogversion(void) {
	struct stat st;
	uint8_t buff[32800];    // 32800 = 32768 + 32
	uint64_t size;
	uint32_t buffpos;
	uint64_t lastnewline;
	int fd;
	lastlogversion = 0;

	if ((stat(kMetadataMlFilename, &st) < 0) || (st.st_size == 0) || ((st.st_mode & S_IFMT) != S_IFREG)) {
		return;
	}

	fd = open(kChangelogMlFilename, O_RDWR);
	if (fd<0) {
		return;
	}
	fstat(fd,&st);
	size = st.st_size;
	memset(buff,0,32);
	lastnewline = 0;
	while (size>0 && size+200000>(uint64_t)(st.st_size)) {
		if (size>32768) {
			memcpy(buff+32768,buff,32);
			size-=32768;
			lseek(fd,size,SEEK_SET);
			if (read(fd,buff,32768)!=32768) {
				lastlogversion = 0;
				close(fd);
				return;
			}
			buffpos = 32768;
		} else {
			memmove(buff+size,buff,32);
			lseek(fd,0,SEEK_SET);
			if (read(fd,buff,size)!=(ssize_t)size) {
				lastlogversion = 0;
				close(fd);
				return;
			}
			buffpos = size;
			size = 0;
		}
		// size = position in file of first byte in buff
		// buffpos = position of last byte in buff to search
		while (buffpos>0) {
			buffpos--;
			if (buff[buffpos]=='\n') {
				if (lastnewline==0) {
					lastnewline = size + buffpos;
				} else {
					if (lastnewline+1 != (uint64_t)(st.st_size)) {  // garbage at the end of file - truncate
						if (ftruncate(fd,lastnewline+1)<0) {
							lastlogversion = 0;
							close(fd);
							return;
						}
					}
					buffpos++;
					while (buffpos<32800 && buff[buffpos]>='0' && buff[buffpos]<='9') {
						lastlogversion *= 10;
						lastlogversion += buff[buffpos]-'0';
						buffpos++;
					}
					if (buffpos==32800 || buff[buffpos]!=':') {
						lastlogversion = 0;
					}
					close(fd);
					return;
				}
			}
		}
	}
	close(fd);
	return;
}
#endif /* #ifdef METALOGGER */

uint8_t* masterconn_createpacket(masterconn *eptr,uint32_t type,uint32_t size) {
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

void masterconn_createpacket(masterconn *eptr, std::vector<uint8_t> data) {
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

void masterconn_sendregister(masterconn *eptr) {
	uint8_t *buff;

	eptr->downloading=0;
	eptr->metafd=-1;

#ifndef METALOGGER
	// shadow master registration
	uint64_t metadataVersion = 0;
	if (eptr->state == MasterConnectionState::kSynchronized) {
		metadataVersion = fs_getversion();
	}
	auto request = mltoma::registerShadow::build(LIZARDFS_VERSHEX, Timeout * 1000, metadataVersion);
	masterconn_createpacket(eptr, std::move(request));
	return;
#endif

	if (lastlogversion>0) {
		buff = masterconn_createpacket(eptr,MLTOMA_REGISTER,1+4+2+8);
		put8bit(&buff,2);
		put16bit(&buff,LIZARDFS_PACKAGE_VERSION_MAJOR);
		put8bit(&buff,LIZARDFS_PACKAGE_VERSION_MINOR);
		put8bit(&buff,LIZARDFS_PACKAGE_VERSION_MICRO);
		put16bit(&buff,Timeout);
		put64bit(&buff,lastlogversion);
	} else {
		buff = masterconn_createpacket(eptr,MLTOMA_REGISTER,1+4+2);
		put8bit(&buff,1);
		put16bit(&buff,LIZARDFS_PACKAGE_VERSION_MAJOR);
		put8bit(&buff,LIZARDFS_PACKAGE_VERSION_MINOR);
		put8bit(&buff,LIZARDFS_PACKAGE_VERSION_MICRO);
		put16bit(&buff,Timeout);
	}
}

namespace {
#ifdef METALOGGER
	const std::string metadataFilename = kMetadataMlFilename;
	const std::string metadataTmpFilename = kMetadataMlTmpFilename;
	const std::string changelogFilename = kChangelogMlFilename;
	const std::string changelogTmpFilename = kChangelogMlTmpFilename;
	const std::string sessionsFilename = kSessionsMlFilename;
	const std::string sessionsTmpFilename = kSessionsMlTmpFilename;
#else /* #ifdef METALOGGER */
	const std::string metadataFilename = kMetadataFilename;
	const std::string metadataTmpFilename = kMetadataTmpFilename;
	const std::string changelogFilename = kChangelogFilename;
	const std::string changelogTmpFilename = kChangelogTmpFilename;
	const std::string sessionsFilename = kSessionsFilename;
	const std::string sessionsTmpFilename = kSessionsTmpFilename;
#endif /* #else #ifdef METALOGGER */
}

void masterconn_kill_session(masterconn* eptr) {
	if (eptr->mode != FREE) {
		eptr->mode = KILL;
	}
}

void masterconn_force_metadata_download(masterconn* eptr) {
#ifndef METALOGGER
	eptr->state = MasterConnectionState::kNone;
	fs_unload();
	restore_reset();
#endif
	lastlogversion = 0;
	masterconn_kill_session(eptr);
}

void masterconn_request_metadata_dump(masterconn* eptr) {
	masterconn_createpacket(eptr, mltoma::changelogApplyError::build(eptr->error_status));
	eptr->state = MasterConnectionState::kDumpRequestPending;
	eptr->changelog_apply_error_packet_time.reset();
}

void masterconn_handle_changelog_apply_error(masterconn* eptr, uint8_t status) {
	if (eptr->version <= lizardfsVersion(2, 5, 0)) {
		lzfs_pretty_syslog(LOG_NOTICE, "Dropping in-memory metadata and starting download from master");
		masterconn_force_metadata_download(eptr);
	} else {
		lzfs_pretty_syslog(LOG_NOTICE, "Waiting for master to produce up-to-date metadata image");
		eptr->error_status = status;
		masterconn_request_metadata_dump(eptr);
	}
}

#ifndef METALOGGER
void masterconn_int_send_matoclport(masterconn* eptr) {
	static std::string previousPort = "";
	if (eptr->version < LIZARDFS_VERSION(2, 5, 5)) {
		return;
	}
	std::string portStr = cfg_getstring("MATOCL_LISTEN_PORT", "9421");
	static uint16_t port = 0;
	if (portStr != previousPort) {
		if (tcpresolve(nullptr, portStr.c_str(), nullptr, &port, false) < 0) {
			lzfs_pretty_syslog(LOG_WARNING, "Cannot resolve MATOCL_LISTEN_PORT: %s", portStr.c_str());
			return;
		}
		previousPort = portStr;
	}
	masterconn_createpacket(eptr, mltoma::matoclport::build(port));
}

void masterconn_registered(masterconn *eptr, const uint8_t *data, uint32_t length) {
	PacketVersion responseVersion;
	deserializePacketVersionNoHeader(data, length, responseVersion);
	if (responseVersion == matoml::registerShadow::kStatusPacketVersion) {
		uint8_t status;
		matoml::registerShadow::deserialize(data, length, status);
		lzfs_pretty_syslog(LOG_NOTICE, "Cannot register to master: %s", lizardfs_error_string(status));
		eptr->mode = KILL;
	} else if (responseVersion == matoml::registerShadow::kResponsePacketVersion) {
		uint32_t masterVersion;
		uint64_t masterMetadataVersion;
		matoml::registerShadow::deserialize(data, length, masterVersion, masterMetadataVersion);
		eptr->version = masterVersion;
		masterconn_int_send_matoclport(eptr);
		if ((eptr->state == MasterConnectionState::kSynchronized) && (fs_getversion() != masterMetadataVersion)) {
			masterconn_force_metadata_download(eptr);
		}
	} else {
		lzfs_pretty_syslog(LOG_NOTICE, "Unknown register response: #%u", unsigned(responseVersion));
	}
}
#endif

void masterconn_metachanges_log(masterconn *eptr,const uint8_t *data,uint32_t length) {
	if ((length == 1) && (data[0] == FORCE_LOG_ROTATE)) {
#ifdef METALOGGER
		// In metalogger rotates are forced by the master server. Shadow masters rotate changelogs
		// every hour -- when creating a new metadata file.
		changelog_rotate();
#endif /* #ifdef METALOGGER */
		return;
	}
	if (length<10) {
		lzfs_pretty_syslog(LOG_NOTICE,"MATOML_METACHANGES_LOG - wrong size (%" PRIu32 "/9+data)",length);
		eptr->mode = KILL;
		return;
	}
	if (data[0]!=0xFF) {
		lzfs_pretty_syslog(LOG_NOTICE,"MATOML_METACHANGES_LOG - wrong packet");
		eptr->mode = KILL;
		return;
	}
	if (data[length-1]!='\0') {
		lzfs_pretty_syslog(LOG_NOTICE,"MATOML_METACHANGES_LOG - invalid string");
		eptr->mode = KILL;
		return;
	}

	data++;
	uint64_t version = get64bit(&data);
	const char* changelogEntry = reinterpret_cast<const char*>(data);

	if ((lastlogversion > 0) && (version != (lastlogversion + 1))) {
		lzfs_pretty_syslog(LOG_WARNING, "some changes lost: [%" PRIu64 "-%" PRIu64 "], download metadata again",lastlogversion,version-1);
		masterconn_handle_changelog_apply_error(eptr, LIZARDFS_ERROR_METADATAVERSIONMISMATCH);
		return;
	}

#ifndef METALOGGER
	if (eptr->state == MasterConnectionState::kSynchronized) {
		std::string buf(": ");
		buf.append(changelogEntry);
		static char const network[] = "network";
		uint8_t status;
		if ((status = restore(network, version, buf.c_str(),
				RestoreRigor::kDontIgnoreAnyErrors)) != LIZARDFS_STATUS_OK) {
			lzfs_pretty_syslog(LOG_WARNING, "malformed changelog sent by the master server, can't apply it. status: %s",
					lizardfs_error_string(status));
			masterconn_handle_changelog_apply_error(eptr, status);
			return;
		}
	}
#endif /* #ifndef METALOGGER */
	changelog(version, changelogEntry);
	lastlogversion = version;
}

void masterconn_end_session(masterconn *eptr, const uint8_t* data, uint32_t length) {
	matoml::endSession::deserialize(data, length); // verify the empty packet
	lzfs_pretty_syslog(LOG_NOTICE, "Master server is terminating; closing the connection...");
	masterconn_kill_session(eptr);
}

int masterconn_download_end(masterconn *eptr) {
	eptr->downloading=0;
	masterconn_createpacket(eptr,MLTOMA_DOWNLOAD_END,0);
	if (eptr->metafd>=0) {
		if (close(eptr->metafd)<0) {
			lzfs_silent_errlog(LOG_NOTICE,"error closing metafile");
			eptr->metafd=-1;
			return -1;
		}
		eptr->metafd=-1;
	}
	return 0;
}

void masterconn_download_init(masterconn *eptr,uint8_t filenum) {
	uint8_t *ptr;
//      syslog(LOG_NOTICE,"download_init %d",filenum);
	if ((eptr->mode==HEADER || eptr->mode==DATA) && eptr->downloading==0) {
//              syslog(LOG_NOTICE,"sending packet");
		ptr = masterconn_createpacket(eptr,MLTOMA_DOWNLOAD_START,1);
		put8bit(&ptr,filenum);
		eptr->downloading=filenum;
		if (filenum == DOWNLOAD_METADATA_MFS) {
			masterconnsingleton->state = MasterConnectionState::kDownloading;
		}
	}
}

void masterconn_metadownloadinit() {
	masterconn_download_init(masterconnsingleton, DOWNLOAD_METADATA_MFS);
}

void masterconn_sessionsdownloadinit(void) {
	if (masterconnsingleton->state == MasterConnectionState::kSynchronized) {
		masterconn_download_init(masterconnsingleton, DOWNLOAD_SESSIONS_MFS);
	}
}

int masterconn_metadata_check(const std::string& name) {
	try {
		metadataGetVersion(name);
		return 0;
	} catch (MetadataCheckException& ex) {
		lzfs_pretty_syslog(LOG_NOTICE, "Verification of the downloaded metadata file failed: %s", ex.what());
		return -1;
	}
}

void masterconn_download_next(masterconn *eptr) {
	uint8_t *ptr;
	uint8_t filenum;
	int64_t dltime;
	if (eptr->dloffset>=eptr->filesize) {   // end of file
		filenum = eptr->downloading;
		if (masterconn_download_end(eptr)<0) {
			return;
		}
		dltime = eventloop_utime()-eptr->dlstartuts;
		if (dltime<=0) {
			dltime=1;
		}
		std::string changelogFilename_1 = changelogFilename + ".1";
		std::string changelogFilename_2 = changelogFilename + ".2";
		lzfs_pretty_syslog(LOG_NOTICE, "%s downloaded %" PRIu64 "B/%" PRIu64 ".%06" PRIu32 "s (%.3f MB/s)",
				(filenum == DOWNLOAD_METADATA_MFS) ? "metadata" :
				(filenum == DOWNLOAD_SESSIONS_MFS) ? "sessions" :
				(filenum == DOWNLOAD_CHANGELOG_MFS) ? changelogFilename_1.c_str() :
				(filenum == DOWNLOAD_CHANGELOG_MFS_1) ? changelogFilename_2.c_str() : "???",
				eptr->filesize, dltime/1000000, (uint32_t)(dltime%1000000),
				(double)(eptr->filesize) / (double)(dltime));
		if (filenum == DOWNLOAD_METADATA_MFS) {
			if (masterconn_metadata_check(metadataTmpFilename) == 0) {
				if (BackMetaCopies>0) {
					rotateFiles(metadataFilename, BackMetaCopies, 1);
				}
				if (rename(metadataTmpFilename.c_str(), metadataFilename.c_str()) < 0) {
					lzfs_pretty_syslog(LOG_NOTICE,"can't rename downloaded metadata - do it manually before next download");
				}
			}
			masterconn_download_init(eptr, DOWNLOAD_CHANGELOG_MFS);
		} else if (filenum == DOWNLOAD_CHANGELOG_MFS) {
			if (rename(changelogTmpFilename.c_str(), changelogFilename_1.c_str()) < 0) {
				lzfs_pretty_syslog(LOG_NOTICE,"can't rename downloaded changelog - do it manually before next download");
			}
			masterconn_download_init(eptr, DOWNLOAD_CHANGELOG_MFS_1);
		} else if (filenum == DOWNLOAD_CHANGELOG_MFS_1) {
			if (rename(changelogTmpFilename.c_str(), changelogFilename_2.c_str()) < 0) {
				lzfs_pretty_syslog(LOG_NOTICE,"can't rename downloaded changelog - do it manually before next download");
			}
			masterconn_download_init(eptr, DOWNLOAD_SESSIONS_MFS);
		} else if (filenum == DOWNLOAD_SESSIONS_MFS) {
			if (rename(sessionsTmpFilename.c_str(), sessionsFilename.c_str()) < 0) {
				lzfs_pretty_syslog(LOG_NOTICE,"can't rename downloaded sessions - do it manually before next download");
			} else {
#ifndef METALOGGER
				/*
				 * We can have other state if we are synchronized or we got changelog apply error
				 * during independent sessions download session.
				 */
				if (eptr->state == MasterConnectionState::kDownloading) {
					try {
						fs_loadall();
						lastlogversion = fs_getversion() - 1;
						lzfs_pretty_syslog(LOG_NOTICE, "synced at version = %" PRIu64, lastlogversion);
						eptr->state = MasterConnectionState::kSynchronized;
					} catch (Exception& ex) {
						lzfs_pretty_syslog(LOG_WARNING, "can't load downloaded metadata and changelogs: %s",
								ex.what());
						uint8_t status = ex.status();
						if (status == LIZARDFS_STATUS_OK) {
							// unknown error - tell the master to apply changelogs and hope that
							// all will be good
							status = LIZARDFS_ERROR_CHANGELOGINCONSISTENT;
						}
						masterconn_handle_changelog_apply_error(eptr, status);
					}
				}
#else /* #ifndef METALOGGER */
				eptr->state = MasterConnectionState::kSynchronized;
#endif /* #else #ifndef METALOGGER */
			}
		}
	} else {        // send request for next data packet
		ptr = masterconn_createpacket(eptr,MLTOMA_DOWNLOAD_DATA,12);
		put64bit(&ptr,eptr->dloffset);
		if (eptr->filesize-eptr->dloffset>META_DL_BLOCK) {
			put32bit(&ptr,META_DL_BLOCK);
		} else {
			put32bit(&ptr,eptr->filesize-eptr->dloffset);
		}
	}
}

void masterconn_download_start(masterconn *eptr,const uint8_t *data,uint32_t length) {
	if (length!=1 && length!=8) {
		lzfs_pretty_syslog(LOG_NOTICE,"MATOML_DOWNLOAD_START - wrong size (%" PRIu32 "/1|8)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	if (length==1) {
		eptr->downloading=0;
		lzfs_pretty_syslog(LOG_NOTICE,"download start error");
		return;
	}
#ifndef METALOGGER
	// We are a shadow master and we are going to do some changes in the data dir right now
	fs_erase_message_from_lockfile();
#endif
	eptr->filesize = get64bit(&data);
	eptr->dloffset = 0;
	eptr->downloadretrycnt = 0;
	eptr->dlstartuts = eventloop_utime();
	if (eptr->downloading == DOWNLOAD_METADATA_MFS) {
		eptr->metafd = open(metadataTmpFilename.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 0666);
	} else if (eptr->downloading == DOWNLOAD_SESSIONS_MFS) {
		eptr->metafd = open(sessionsTmpFilename.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 0666);
	} else if ((eptr->downloading == DOWNLOAD_CHANGELOG_MFS)
			|| (eptr->downloading == DOWNLOAD_CHANGELOG_MFS_1)) {
		eptr->metafd = open(changelogTmpFilename.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 0666);
	} else {
		lzfs_pretty_syslog(LOG_NOTICE,"unexpected MATOML_DOWNLOAD_START packet");
		eptr->mode = KILL;
		return;
	}
	if (eptr->metafd<0) {
		lzfs_silent_errlog(LOG_NOTICE,"error opening metafile");
		masterconn_download_end(eptr);
		return;
	}
	masterconn_download_next(eptr);
}

void masterconn_download_data(masterconn *eptr,const uint8_t *data,uint32_t length) {
	uint64_t offset;
	uint32_t leng;
	uint32_t crc;
	ssize_t ret;
	if (eptr->metafd<0) {
		lzfs_pretty_syslog(LOG_NOTICE,"MATOML_DOWNLOAD_DATA - file not opened");
		eptr->mode = KILL;
		return;
	}
	if (length<16) {
		lzfs_pretty_syslog(LOG_NOTICE,"MATOML_DOWNLOAD_DATA - wrong size (%" PRIu32 "/16+data)",length);
		eptr->mode = KILL;
		return;
	}
	passert(data);
	offset = get64bit(&data);
	leng = get32bit(&data);
	crc = get32bit(&data);
	if (leng+16!=length) {
		lzfs_pretty_syslog(LOG_NOTICE,"MATOML_DOWNLOAD_DATA - wrong size (%" PRIu32 "/16+%" PRIu32 ")",length,leng);
		eptr->mode = KILL;
		return;
	}
	if (offset!=eptr->dloffset) {
		lzfs_pretty_syslog(LOG_NOTICE,"MATOML_DOWNLOAD_DATA - unexpected file offset (%" PRIu64 "/%" PRIu64 ")",offset,eptr->dloffset);
		eptr->mode = KILL;
		return;
	}
	if (offset+leng>eptr->filesize) {
		lzfs_pretty_syslog(LOG_NOTICE,"MATOML_DOWNLOAD_DATA - unexpected file size (%" PRIu64 "/%" PRIu64 ")",offset+leng,eptr->filesize);
		eptr->mode = KILL;
		return;
	}
#ifdef LIZARDFS_HAVE_PWRITE
	ret = pwrite(eptr->metafd,data,leng,offset);
#else /* LIZARDFS_HAVE_PWRITE */
	lseek(eptr->metafd,offset,SEEK_SET);
	ret = write(eptr->metafd,data,leng);
#endif /* LIZARDFS_HAVE_PWRITE */
	if (ret!=(ssize_t)leng) {
		lzfs_silent_errlog(LOG_NOTICE,"error writing metafile");
		if (eptr->downloadretrycnt>=5) {
			masterconn_download_end(eptr);
		} else {
			eptr->downloadretrycnt++;
			masterconn_download_next(eptr);
		}
		return;
	}
	if (crc!=mycrc32(0,data,leng)) {
		lzfs_pretty_syslog(LOG_NOTICE,"metafile data crc error");
		if (eptr->downloadretrycnt>=5) {
			masterconn_download_end(eptr);
		} else {
			eptr->downloadretrycnt++;
			masterconn_download_next(eptr);
		}
		return;
	}
	if (fsync(eptr->metafd)<0) {
		lzfs_silent_errlog(LOG_NOTICE,"error syncing metafile");
		if (eptr->downloadretrycnt>=5) {
			masterconn_download_end(eptr);
		} else {
			eptr->downloadretrycnt++;
			masterconn_download_next(eptr);
		}
		return;
	}
	eptr->dloffset+=leng;
	eptr->downloadretrycnt=0;
	masterconn_download_next(eptr);
}

void masterconn_changelog_apply_error(masterconn *eptr, const uint8_t *data, uint32_t length) {
	uint8_t status;
	matoml::changelogApplyError::deserialize(data, length, status);
	lzfs_silent_syslog(LOG_DEBUG, "master.matoml_changelog_apply_error status: %u", status);
	if (status == LIZARDFS_STATUS_OK) {
		masterconn_force_metadata_download(eptr);
	} else if (status == LIZARDFS_ERROR_DELAYED) {
		eptr->state = MasterConnectionState::kLimbo;
		lzfs_pretty_syslog(LOG_NOTICE, "Master temporarily refused to produce a new metadata image");
	} else {
		eptr->state = MasterConnectionState::kLimbo;
		lzfs_pretty_syslog(LOG_NOTICE, "Master failed to produce a new metadata image: %s", lizardfs_error_string(status));
	}
}

void masterconn_beforeclose(masterconn *eptr) {
	if (eptr->metafd>=0) {
		close(eptr->metafd);
		eptr->metafd=-1;
		unlink(metadataTmpFilename.c_str());
		unlink(sessionsTmpFilename.c_str());
		unlink(changelogTmpFilename.c_str());
	}
}

void masterconn_gotpacket(masterconn *eptr,uint32_t type,const uint8_t *data,uint32_t length) {
	try {
		switch (type) {
			case ANTOAN_NOP:
				break;
			case ANTOAN_UNKNOWN_COMMAND: // for future use
				break;
			case ANTOAN_BAD_COMMAND_SIZE: // for future use
				break;
#ifndef METALOGGER
			case LIZ_MATOML_REGISTER_SHADOW:
				masterconn_registered(eptr,data,length);
				break;
#endif
			case MATOML_METACHANGES_LOG:
				masterconn_metachanges_log(eptr,data,length);
				break;
			case LIZ_MATOML_END_SESSION:
				masterconn_end_session(eptr,data,length);
				break;
			case MATOML_DOWNLOAD_START:
				masterconn_download_start(eptr,data,length);
				break;
			case MATOML_DOWNLOAD_DATA:
				masterconn_download_data(eptr,data,length);
				break;
			case LIZ_MATOML_CHANGELOG_APPLY_ERROR:
				masterconn_changelog_apply_error(eptr, data, length);
				break;
			default:
				lzfs_pretty_syslog(LOG_NOTICE,"got unknown message (type:%" PRIu32 ")",type);
				eptr->mode = KILL;
				break;
		}
	} catch (IncorrectDeserializationException& ex) {
		lzfs_pretty_syslog(LOG_NOTICE, "Packet 0x%" PRIX32 " - can't deserialize: %s", type, ex.what());
		eptr->mode = KILL;
	}
}

void masterconn_term(void) {
	if (!masterconnsingleton) {
		return;
	}
	packetstruct *pptr,*paptr;
	masterconn *eptr = masterconnsingleton;

	if (eptr->mode!=FREE) {
		tcpclose(eptr->sock);
		if (eptr->mode!=CONNECTING) {
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
		}
	}

	delete masterconnsingleton;
	masterconnsingleton = NULL;
}

void masterconn_connected(masterconn *eptr) {
	tcpnodelay(eptr->sock);
	eptr->mode=HEADER;
	eptr->version = 0;
	eptr->inputpacket.next = NULL;
	eptr->inputpacket.bytesleft = 8;
	eptr->inputpacket.startptr = eptr->hdrbuff;
	eptr->inputpacket.packet = NULL;
	eptr->outputhead = NULL;
	eptr->outputtail = &(eptr->outputhead);

	masterconn_sendregister(eptr);
	if (lastlogversion==0) {
		masterconn_metadownloadinit();
	} else if (eptr->state == MasterConnectionState::kDumpRequestPending) {
		masterconn_request_metadata_dump(eptr);
	}
	eptr->lastread = eptr->lastwrite = eventloop_time();
}

int masterconn_initconnect(masterconn *eptr) {
	int status;
	if (eptr->masteraddrvalid==0) {
		uint32_t mip,bip;
		uint16_t mport;
		if (tcpresolve(BindHost.c_str(), NULL, &bip, NULL, 1)>=0) {
			eptr->bindip = bip;
		} else {
			eptr->bindip = 0;
		}
		if (tcpresolve(MasterHost.c_str(), MasterPort.c_str(), &mip, &mport, 0)>=0) {
			eptr->masterip = mip;
			eptr->masterport = mport;
			eptr->masteraddrvalid = 1;
		} else {
			lzfs_pretty_syslog(LOG_WARNING,
					"can't resolve master host/port (%s:%s)",
					MasterHost.c_str(), MasterPort.c_str());
			return -1;
		}
	}
	eptr->sock=tcpsocket();
	if (eptr->sock<0) {
		lzfs_pretty_errlog(LOG_WARNING,"create socket, error");
		return -1;
	}
	if (tcpnonblock(eptr->sock)<0) {
		lzfs_pretty_errlog(LOG_WARNING,"set nonblock, error");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		return -1;
	}
	if (eptr->bindip>0) {
		if (tcpnumbind(eptr->sock,eptr->bindip,0)<0) {
			lzfs_pretty_errlog(LOG_WARNING,"can't bind socket to given ip");
			tcpclose(eptr->sock);
			eptr->sock = -1;
			return -1;
		}
	}
	status = tcpnumconnect(eptr->sock,eptr->masterip,eptr->masterport);
	if (status<0) {
		lzfs_pretty_errlog(LOG_WARNING,"connect failed, error");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		eptr->masteraddrvalid = 0;
		return -1;
	}
	if (status==0) {
		lzfs_pretty_syslog(LOG_NOTICE,"connected to Master immediately");
		masterconn_connected(eptr);
	} else {
		eptr->mode = CONNECTING;
		lzfs_pretty_syslog_attempt(LOG_NOTICE,"connecting to Master");
	}
	return 0;
}

void masterconn_connecttest(masterconn *eptr) {
	int status;

	status = tcpgetstatus(eptr->sock);
	if (status) {
		lzfs_silent_errlog(LOG_WARNING,"connection failed, error");
		tcpclose(eptr->sock);
		eptr->sock = -1;
		eptr->mode = FREE;
		eptr->masteraddrvalid = 0;
		eptr->version = 0;
	} else {
		lzfs_pretty_syslog(LOG_NOTICE,"connected to Master");
		masterconn_connected(eptr);
	}
}

void masterconn_read(masterconn *eptr) {
	SignalLoopWatchdog watchdog;
	int32_t i;
	uint32_t type,size;
	const uint8_t *ptr;

	watchdog.start();
	while (eptr->mode != KILL) {
		i=read(eptr->sock,eptr->inputpacket.startptr,eptr->inputpacket.bytesleft);
		if (i==0) {
			lzfs_pretty_syslog(LOG_NOTICE,"connection was reset by Master");
			masterconn_kill_session(eptr);
			return;
		}
		if (i<0) {
			if (errno!=EAGAIN) {
				lzfs_silent_errlog(LOG_NOTICE,"read from Master error");
				masterconn_kill_session(eptr);
			}
			return;
		}
		stats_bytesin+=i;
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
					lzfs_pretty_syslog(LOG_WARNING,"Master packet too long (%" PRIu32 "/%u)",size,MaxPacketSize);
					masterconn_kill_session(eptr);
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

			masterconn_gotpacket(eptr,type,eptr->inputpacket.packet,size);

			if (eptr->inputpacket.packet) {
				free(eptr->inputpacket.packet);
			}
			eptr->inputpacket.packet=NULL;
		}

		if (watchdog.expired()) {
			break;
		}
	}
}

void masterconn_write(masterconn *eptr) {
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
				lzfs_silent_errlog(LOG_NOTICE,"write to Master error");
				eptr->mode = KILL;
			}
			return;
		}
		stats_bytesout+=i;
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

void masterconn_wantexit(void) {
	if (masterconnsingleton) {
		masterconn_kill_session(masterconnsingleton);
	}
}

int masterconn_canexit(void) {
	return !masterconnsingleton || masterconnsingleton->mode == FREE;
}

void masterconn_desc(std::vector<pollfd> &pdesc) {
	if (!masterconnsingleton) {
		return;
	}
	masterconn *eptr = masterconnsingleton;

	eptr->pdescpos = -1;
	if (eptr->mode==FREE || eptr->sock<0) {
		return;
	}
	if (eptr->mode==HEADER || eptr->mode==DATA) {
		pdesc.push_back({eptr->sock,POLLIN,0});
		eptr->pdescpos = pdesc.size() - 1;
	}
	if (((eptr->mode==HEADER || eptr->mode==DATA) && eptr->outputhead!=NULL) || eptr->mode==CONNECTING) {
		if (eptr->pdescpos>=0) {
			pdesc[eptr->pdescpos].events |= POLLOUT;
		} else {
			pdesc.push_back({eptr->sock,POLLOUT,0});
			eptr->pdescpos = pdesc.size() - 1;
		}
	}
}

void masterconn_serve(const std::vector<pollfd> &pdesc) {
	if (!masterconnsingleton) {
		return;
	}
	uint32_t now=eventloop_time();
	packetstruct *pptr,*paptr;
	masterconn *eptr = masterconnsingleton;

	if (eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & (POLLHUP | POLLERR))) {
		if (eptr->mode==CONNECTING) {
			masterconn_connecttest(eptr);
		} else {
			eptr->mode = KILL;
		}
	}
	if (eptr->mode==CONNECTING) {
		if (eptr->sock>=0 && eptr->pdescpos>=0 && (pdesc[eptr->pdescpos].revents & POLLOUT)) { // FD_ISSET(eptr->sock,wset)) {
			masterconn_connecttest(eptr);
		}
	} else {
		if (eptr->pdescpos>=0) {
			if ((eptr->mode==HEADER || eptr->mode==DATA) && (pdesc[eptr->pdescpos].revents & POLLIN)) { // FD_ISSET(eptr->sock,rset)) {
				eptr->lastread = now;
				masterconn_read(eptr);
			}
			if ((eptr->mode==HEADER || eptr->mode==DATA) && (pdesc[eptr->pdescpos].revents & POLLOUT)) { // FD_ISSET(eptr->sock,wset)) {
				eptr->lastwrite = now;
				masterconn_write(eptr);
			}
			if ((eptr->mode==HEADER || eptr->mode==DATA) && eptr->lastread+Timeout<now) {
				eptr->mode = KILL;
			}
			if ((eptr->mode==HEADER || eptr->mode==DATA) && eptr->lastwrite+(Timeout/3)<now && eptr->outputhead==NULL) {
				masterconn_createpacket(eptr,ANTOAN_NOP,0);
			}
		}
	}
	if (eptr->mode == KILL) {
		masterconn_beforeclose(eptr);
		tcpclose(eptr->sock);
		eptr->sock = -1;
		if (eptr->inputpacket.packet) {
			free(eptr->inputpacket.packet);
			eptr->inputpacket.packet = NULL;
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
		eptr->outputhead = NULL;
		eptr->mode = FREE;
		eptr->version = 0;
	}
}

void masterconn_reconnect(void) {
	if (!masterconnsingleton) {
		return;
	}
	masterconn *eptr = masterconnsingleton;
	if (eptr->mode == FREE && gExitingStatus == ExitingStatus::kRunning) {
		masterconn_initconnect(eptr);
	}
	if ((eptr->mode == HEADER || eptr->mode == DATA) && eptr->state == MasterConnectionState::kLimbo) {
		if (eptr->changelog_apply_error_packet_time.expired()) {
			masterconn_request_metadata_dump(eptr);
		}
	}
}

void masterconn_become_master() {
	if (!masterconnsingleton) {
		return;
	}
	masterconn *eptr = masterconnsingleton;
	eventloop_timeunregister(eptr->sessionsdownloadinit_handle);
	eventloop_timeunregister(eptr->metachanges_flush_handle);
	masterconn_term();
	return;
}

void masterconn_reload(void) {
	if (!masterconnsingleton) {
		return;
	}
	masterconn *eptr = masterconnsingleton;
	uint32_t ReconnectionDelay;

	std::string newMasterHost = cfg_getstring("MASTER_HOST","mfsmaster");
	std::string newMasterPort = cfg_getstring("MASTER_PORT","9419");
	std::string newBindHost = cfg_getstring("BIND_HOST","*");

	if (newMasterHost != MasterHost || newMasterPort != MasterPort || newBindHost != BindHost) {
		MasterHost = newMasterHost;
		MasterPort = newMasterPort;
		BindHost = newBindHost;
		eptr->masteraddrvalid = 0;
		if (eptr->mode != FREE) {
			eptr->mode = KILL;
		}
	}

	Timeout = cfg_getuint32("MASTER_TIMEOUT",60);
	BackMetaCopies = cfg_getuint32("BACK_META_KEEP_PREVIOUS",3);
	ReconnectionDelay = cfg_getuint32("MASTER_RECONNECTION_DELAY",1);

	if (Timeout>65536) {
		Timeout=65535;
	}
	if (Timeout<10) {
		Timeout=10;
	}
	if (BackMetaCopies>99) {
		BackMetaCopies=99;
	}

#ifdef METALOGGER
	uint32_t metadataDownloadFreq;
	metadataDownloadFreq = cfg_getuint32("META_DOWNLOAD_FREQ",24);
	if (metadataDownloadFreq > (changelog_get_back_logs_config_value() / 2)) {
		metadataDownloadFreq = (changelog_get_back_logs_config_value() / 2);
	}
#endif /* #ifdef METALOGGER */

	eventloop_timechange(reconnect_hook,TIMEMODE_RUN_LATE,ReconnectionDelay,0);
#ifdef METALOGGER
	eventloop_timechange(download_hook,TIMEMODE_RUN_LATE,metadataDownloadFreq*3600,630);
#endif /* #ifndef METALOGGER */

#ifndef METALOGGER
	masterconn_int_send_matoclport(masterconnsingleton);
#endif /* #ifndef METALOGGER */
}

int masterconn_init(void) {
	uint32_t ReconnectionDelay;
#ifndef METALOGGER
	if (metadataserver::getPersonality() != metadataserver::Personality::kShadow) {
		return 0;
	}
#endif /* #ifndef METALOGGER */
	masterconn *eptr;

	ReconnectionDelay = cfg_getuint32("MASTER_RECONNECTION_DELAY", 1);
	MasterHost = cfg_getstring("MASTER_HOST","mfsmaster");
	MasterPort = cfg_getstring("MASTER_PORT","9419");
	BindHost = cfg_getstring("BIND_HOST","*");
	Timeout = cfg_getuint32("MASTER_TIMEOUT",60);
	BackMetaCopies = cfg_getuint32("BACK_META_KEEP_PREVIOUS",3);

	if (Timeout>65536) {
		Timeout=65535;
	}
	if (Timeout<10) {
		Timeout=10;
	}

#ifdef METALOGGER
	changelog_init(kChangelogMlFilename, 5, 1000); // may throw
	changelog_disable_flush(); // metalogger does it once a second
	uint32_t metadataDownloadFreq;
	metadataDownloadFreq = cfg_getuint32("META_DOWNLOAD_FREQ",24);
	if (metadataDownloadFreq > (changelog_get_back_logs_config_value() / 2)) {
		metadataDownloadFreq = (changelog_get_back_logs_config_value() / 2);
	}
#endif /* #ifdef METALOGGER */

	eptr = masterconnsingleton = new masterconn();
	passert(eptr);

	eptr->masteraddrvalid = 0;
	eptr->mode = FREE;
	eptr->pdescpos = -1;
	eptr->metafd = -1;
	eptr->version = 0;
	eptr->sock  = -1;
	eptr->state = MasterConnectionState::kNone;
#ifdef METALOGGER
	changelogsMigrateFrom_1_6_29("changelog_ml");
	masterconn_findlastlogversion();
#endif /* #ifdef METALOGGER */
	if (masterconn_initconnect(eptr)<0) {
		return -1;
	}
	reconnect_hook = eventloop_timeregister(TIMEMODE_RUN_LATE,ReconnectionDelay,0,masterconn_reconnect);
#ifdef METALOGGER
	download_hook = eventloop_timeregister(TIMEMODE_RUN_LATE,metadataDownloadFreq*3600,630,masterconn_metadownloadinit);
#endif /* #ifdef METALOGGER */
	eventloop_destructregister(masterconn_term);
	eventloop_pollregister(masterconn_desc,masterconn_serve);
	eventloop_reloadregister(masterconn_reload);
	eventloop_wantexitregister(masterconn_wantexit);
	eventloop_canexitregister(masterconn_canexit);
#ifndef METALOGGER
	metadataserver::registerFunctionCalledOnPromotion(masterconn_become_master);
#endif
	eptr->sessionsdownloadinit_handle = eventloop_timeregister(TIMEMODE_RUN_LATE,60,0,masterconn_sessionsdownloadinit);
	eptr->metachanges_flush_handle = eventloop_timeregister(TIMEMODE_RUN_LATE,1,0,changelog_flush);
	return 0;
}

bool masterconn_is_connected() {
	masterconn *eptr = masterconnsingleton;
	return (eptr != nullptr
			&& (eptr->mode == HEADER || eptr->mode == DATA) // socket is connected
			&& eptr->version > 0 // registration was successful
	);
}
