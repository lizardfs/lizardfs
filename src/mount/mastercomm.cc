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

#define MMAP_ALLOC 1

#include <errno.h>
#include <grp.h>
#include <limits.h>
#include <pthread.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/poll.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#ifdef MMAP_ALLOC
#  include <sys/types.h>
#  include <sys/mman.h>
#endif

#include <condition_variable>
#include <mutex>
#include <vector>

#include "common/cltoma_communication.h"
#include "common/datapack.h"
#include "common/matocl_communication.h"
#include "common/md5.h"
#include "common/MFSCommunication.h"
#include "common/sockets.h"
#include "common/strerr.h"
#include "stats.h"

typedef struct _threc {
	pthread_t thid;
	std::mutex mutex;
	std::condition_variable condition;
	uint8_t *obuff;
	uint32_t obuffsize;
	uint32_t odataleng;
	std::vector<uint8_t> inputBuffer;
	uint32_t idataleng;
	uint8_t sent;		// packet was sent
	uint8_t status;		// receive status
	uint8_t rcvd;		// packet was received
	uint8_t waiting;	// thread is waiting for answer

	uint32_t rcvd_cmd;

	uint32_t packetid;	// thread number
	struct _threc *next;
} threc;

typedef struct _acquired_file {
	uint32_t inode;
	uint32_t cnt;
	struct _acquired_file *next;
} acquired_file;


#define DEFAULT_OUTPUT_BUFFSIZE 0x1000
#define DEFAULT_INPUT_BUFFSIZE 0x10000

#define RECEIVE_TIMEOUT 10

static threc *threchead=NULL;

static acquired_file *afhead=NULL;

static int fd;
static bool disconnect;
static time_t lastwrite;
static int sessionlost;

static uint32_t maxretries;

static pthread_t rpthid,npthid;
static std::mutex fdMutex, recMutex, acquiredFileMutex;

static uint32_t sessionid;
static uint32_t masterversion;

static char masterstrip[17];
static uint32_t masterip=0;
static uint16_t masterport=0;
static char srcstrip[17];
static uint32_t srcip=0;

static uint8_t fterm;

void fs_getmasterlocation(uint8_t loc[14]) {
	put32bit(&loc,masterip);
	put16bit(&loc,masterport);
	put32bit(&loc,sessionid);
	put32bit(&loc,masterversion);
}

uint32_t fs_getsrcip() {
	return srcip;
}

enum {
	MASTER_CONNECTS = 0,
	MASTER_BYTESSENT,
	MASTER_BYTESRCVD,
	MASTER_PACKETSSENT,
	MASTER_PACKETSRCVD,
	STATNODES
};

static uint64_t *statsptr[STATNODES];

struct connect_args_t {
	char *bindhostname;
	char *masterhostname;
	char *masterportname;
	uint8_t meta;
	uint8_t clearpassword;
	char *info;
	char *subfolder;
	uint8_t *passworddigest;
};

static struct connect_args_t connect_args;

void master_statsptr_init(void) {
	void *s;
	s = stats_get_subnode(NULL,"master",0);
	statsptr[MASTER_PACKETSRCVD] = stats_get_counterptr(stats_get_subnode(s,"packets_received",0));
	statsptr[MASTER_PACKETSSENT] = stats_get_counterptr(stats_get_subnode(s,"packets_sent",0));
	statsptr[MASTER_BYTESRCVD] = stats_get_counterptr(stats_get_subnode(s,"bytes_received",0));
	statsptr[MASTER_BYTESSENT] = stats_get_counterptr(stats_get_subnode(s,"bytes_sent",0));
	statsptr[MASTER_CONNECTS] = stats_get_counterptr(stats_get_subnode(s,"reconnects",0));
}

void master_stats_inc(uint8_t id) {
	if (id<STATNODES) {
		stats_lock();
		(*statsptr[id])++;
		stats_unlock();
	}
}

void master_stats_add(uint8_t id,uint64_t s) {
	if (id<STATNODES) {
		stats_lock();
		(*statsptr[id])+=s;
		stats_unlock();
	}
}

const char* errtab[]={ERROR_STRINGS};

static inline const char* mfs_strerror(uint8_t status) {
	if (status>ERROR_MAX) {
		status=ERROR_MAX;
	}
	return errtab[status];
}

static inline void setDisconnect(bool value) {
	std::unique_lock<std::mutex> fdLock(fdMutex);
	disconnect = value;
}

void fs_inc_acnt(uint32_t inode) {
	acquired_file *afptr,**afpptr;
	std::unique_lock<std::mutex> acquiredFileLock(acquiredFileMutex);
	afpptr = &afhead;
	while ((afptr=*afpptr)) {
		if (afptr->inode==inode) {
			afptr->cnt++;
			return;
		}
		if (afptr->inode>inode) {
			break;
		}
		afpptr = &(afptr->next);
	}
	afptr = (acquired_file*)malloc(sizeof(acquired_file));
	afptr->inode = inode;
	afptr->cnt = 1;
	afptr->next = *afpptr;
	*afpptr = afptr;
}

void fs_dec_acnt(uint32_t inode) {
	acquired_file *afptr,**afpptr;
	std::unique_lock<std::mutex> afLock(acquiredFileMutex);
	afpptr = &afhead;
	while ((afptr=*afpptr)) {
		if (afptr->inode == inode) {
			if (afptr->cnt<=1) {
				*afpptr = afptr->next;
				free(afptr);
			} else {
				afptr->cnt--;
			}
			return;
		}
		afpptr = &(afptr->next);
	}
}

threc* fs_get_my_threc() {
	pthread_t mythid = pthread_self();
	threc *rec;
	std::unique_lock<std::mutex> recLock(recMutex);
	for (rec = threchead ; rec ; rec = rec->next) {
		if (pthread_equal(rec->thid, mythid)) {
			return rec;
		}
	}
	rec = new threc;
	rec->thid = mythid;
	rec->obuff = NULL;
	rec->obuffsize = 0;
	rec->odataleng = 0;
	rec->idataleng = 0;
	rec->sent = 0;
	rec->status = 0;
	rec->rcvd = 0;
	rec->waiting = 0;
	rec->rcvd_cmd = 0;
	if (threchead==NULL) {
		rec->packetid = 1;
	} else {
		rec->packetid = threchead->packetid + 1;
	}
	rec->next = threchead;
	//syslog(LOG_NOTICE,"mastercomm: create new threc (%" PRIu32 ")",rec->packetid);
	threchead = rec;
	return rec;
}

threc* fs_get_threc_by_id(uint32_t packetid) {
	threc *rec;
	std::unique_lock<std::mutex> recLock(recMutex);
	for (rec = threchead ; rec ; rec=rec->next) {
		if (rec->packetid==packetid) {
			return rec;
		}
	}
	return NULL;
}

void fs_output_buffer_init(threc *rec,uint32_t size) {
	if (size>DEFAULT_OUTPUT_BUFFSIZE) {
#ifdef MMAP_ALLOC
		if (rec->obuff) {
			munmap((void*)(rec->obuff),rec->obuffsize);
		}
		rec->obuff = (uint8_t*) (void*)mmap(NULL,size,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		if (rec->obuff) {
			free(rec->obuff);
		}
		rec->obuff = malloc(size);
#endif
		rec->obuffsize = size;
	} else if (rec->obuffsize!=DEFAULT_OUTPUT_BUFFSIZE) {
#ifdef MMAP_ALLOC
		if (rec->obuff) {
			munmap((void*)(rec->obuff),rec->obuffsize);
		}
		rec->obuff = (uint8_t*) (void*)mmap(NULL,DEFAULT_OUTPUT_BUFFSIZE,PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE,-1,0);
#else
		if (rec->obuff) {
			free(rec->obuff);
		}
		rec->obuff = malloc(DEFAULT_OUTPUT_BUFFSIZE);
#endif
		rec->obuffsize = DEFAULT_OUTPUT_BUFFSIZE;
	}
	if (rec->obuff==NULL) {
		rec->obuffsize = 0;
	}
}

uint8_t* fs_createpacket(threc *rec,uint32_t cmd,uint32_t size) {
	uint8_t *ptr;
	uint32_t hdrsize = size+4;
	std::unique_lock<std::mutex> lock(rec->mutex);
	fs_output_buffer_init(rec,size+12);
	if (rec->obuff==NULL) {
		return NULL;
	}
	ptr = rec->obuff;
	put32bit(&ptr,cmd);
	put32bit(&ptr,hdrsize);
	put32bit(&ptr,rec->packetid);
	rec->odataleng = size+12;
	return ptr;
}

bool fsLizCreatePacket(threc *rec, const std::vector<uint8_t>& message) {
	std::unique_lock<std::mutex> lock(rec->mutex);
	fs_output_buffer_init(rec, message.size());
	if (rec->obuff == NULL) {
		return false;
	}
	memcpy(rec->obuff, message.data(), message.size());
	rec->odataleng = message.size();
	return true;
}

const uint8_t* fs_sendandreceive(threc *rec,uint32_t expected_cmd,uint32_t *answer_leng) {
	uint32_t cnt;
	static uint8_t notsup = ERROR_ENOTSUP;

	for (cnt=0 ; cnt<maxretries ; cnt++) {

		std::unique_lock<std::mutex> fdLock(fdMutex);
		if (sessionlost) {
			return NULL;
		}
		if (fd==-1) {
			fdLock.unlock();
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		//syslog(LOG_NOTICE,"threc(%" PRIu32 ") - sending ...",rec->packetid);
		std::unique_lock<std::mutex> lock(rec->mutex);
		if (tcptowrite(fd,rec->obuff,rec->odataleng,1000)!=(int32_t)(rec->odataleng)) {
			syslog(LOG_WARNING,"tcp send error: %s",strerr(errno));
			disconnect = true;
			lock.unlock();
			fdLock.unlock();
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		rec->rcvd = 0;
		rec->sent = 1;
		lock.unlock();
		master_stats_add(MASTER_BYTESSENT,rec->odataleng);
		master_stats_inc(MASTER_PACKETSSENT);
		lastwrite = time(NULL);
		fdLock.unlock();
		// syslog(LOG_NOTICE,"master: lock: %" PRIu32,rec->packetid);
		lock.lock();
		while (rec->rcvd==0) {
			rec->waiting = 1;
			rec->condition.wait(lock);
			rec->waiting = 0;
		}
		*answer_leng = rec->idataleng;
		// syslog(LOG_NOTICE,"master: unlocked: %" PRIu32,rec->packetid);
		// syslog(LOG_NOTICE,"master: command_info: %" PRIu32 " ; reccmd: %" PRIu32,command_info,rec->cmd);
		if (rec->status!=0) {
			lock.unlock();
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		if (rec->rcvd_cmd==ANTOAN_UNKNOWN_COMMAND || rec->rcvd_cmd==ANTOAN_BAD_COMMAND_SIZE) {
			lock.unlock();
			*answer_leng = 1; // simulate error
			return &notsup; // return ERROR_ENOTSUP in this case
		}
		if (rec->rcvd_cmd!=expected_cmd) {
			lock.unlock();
			setDisconnect(true);
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		lock.unlock();
		//syslog(LOG_NOTICE,"threc(%" PRIu32 ") - received",rec->packetid);
		return rec->inputBuffer.data();
	}
	return NULL;
}

bool fsLizSendAndReceive(threc *rec, uint32_t expectedCommand,
		std::vector<uint8_t>& messageData) {

	uint32_t size;
	const uint8_t* ret = fs_sendandreceive(rec, expectedCommand, &size);
	if (ret == NULL) {
		return false;
	}
	messageData.resize(size);
	memcpy(messageData.data(), ret, size);
	return true;
}

const uint8_t* fs_sendandreceive_any(threc *rec,uint32_t *received_cmd,uint32_t *answer_leng) {
	uint32_t cnt;

	for (cnt=0 ; cnt<maxretries ; cnt++) {
		std::unique_lock<std::mutex> fdLock(fdMutex);

		if (sessionlost) {
			return NULL;
		}
		if (fd==-1) {
			fdLock.unlock();
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		//syslog(LOG_NOTICE,"threc(%" PRIu32 ") - sending ...",rec->packetid);
		std::unique_lock<std::mutex> lock(rec->mutex);
		if (tcptowrite(fd,rec->obuff,rec->odataleng,1000)!=(int32_t)(rec->odataleng)) {
			syslog(LOG_WARNING,"tcp send error: %s",strerr(errno));
			disconnect = true;
			lock.unlock();
			fdLock.unlock();
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		rec->rcvd = 0;
		rec->sent = 1;
		lock.unlock();
		master_stats_add(MASTER_BYTESSENT,rec->odataleng);
		master_stats_inc(MASTER_PACKETSSENT);
		lastwrite = time(NULL);
		fdLock.unlock();
		// syslog(LOG_NOTICE,"master: lock: %" PRIu32,rec->packetid);
		lock.lock();
		while (rec->rcvd==0) {
			rec->waiting = 1;
			rec->condition.wait(lock);
			rec->waiting = 0;
		}
		*answer_leng = rec->idataleng;
		// syslog(LOG_NOTICE,"master: unlocked: %" PRIu32,rec->packetid);
		// syslog(LOG_NOTICE,"master: command_info: %" PRIu32 " ; reccmd: %" PRIu32,command_info,rec->cmd);
		if (rec->status!=0) {
			lock.unlock();
			sleep(1+((cnt<30)?(cnt/3):10));
			continue;
		}
		*received_cmd = rec->rcvd_cmd;
		//syslog(LOG_NOTICE,"threc(%" PRIu32 ") - received",rec->packetid);
		return rec->inputBuffer.data();
	}
	return NULL;
}

int fs_resolve(uint8_t oninit,const char *bindhostname,const char *masterhostname,const char *masterportname) {
	if (bindhostname) {
		if (tcpresolve(bindhostname,NULL,&srcip,NULL,1)<0) {
			if (oninit) {
				fprintf(stderr,"can't resolve source hostname (%s)\n",bindhostname);
			} else {
				syslog(LOG_WARNING,"can't resolve source hostname (%s)",bindhostname);
			}
			return -1;
		}
	} else {
		srcip=0;
	}
	snprintf(srcstrip,17,"%" PRIu32 ".%" PRIu32 ".%" PRIu32 ".%" PRIu32,(srcip>>24)&0xFF,(srcip>>16)&0xFF,(srcip>>8)&0xFF,srcip&0xFF);
	srcstrip[16]=0;

	if (tcpresolve(masterhostname,masterportname,&masterip,&masterport,0)<0) {
		if (oninit) {
			fprintf(stderr,"can't resolve master hostname and/or portname (%s:%s)\n",masterhostname,masterportname);
		} else {
			syslog(LOG_WARNING,"can't resolve master hostname and/or portname (%s:%s)",masterhostname,masterportname);
		}
		return -1;
	}
	snprintf(masterstrip,17,"%" PRIu32 ".%" PRIu32 ".%" PRIu32 ".%" PRIu32,(masterip>>24)&0xFF,(masterip>>16)&0xFF,(masterip>>8)&0xFF,masterip&0xFF);
	masterstrip[16]=0;

	return 0;
}

int fs_connect(uint8_t oninit,struct connect_args_t *cargs) {
	uint32_t i,j;
	uint8_t *wptr,*regbuff;
	md5ctx ctx;
	uint8_t digest[16];
	const uint8_t *rptr;
	uint8_t havepassword;
	uint32_t pleng,ileng;
	uint8_t sesflags;
	uint32_t rootuid,rootgid,mapalluid,mapallgid;
	uint8_t mingoal,maxgoal;
	uint32_t mintrashtime,maxtrashtime;
	const char *sesflagposstrtab[]={SESFLAG_POS_STRINGS};
	const char *sesflagnegstrtab[]={SESFLAG_NEG_STRINGS};
	struct passwd pwd,*pw;
	struct group grp,*gr;
	char pwdgrpbuff[16384];

	if (fs_resolve(oninit,cargs->bindhostname,cargs->masterhostname,cargs->masterportname)<0) {
		return -1;
	}

	havepassword=(cargs->passworddigest==NULL)?0:1;
	ileng=strlen(cargs->info)+1;
	if (cargs->meta) {
		pleng=0;
		regbuff = (uint8_t*) malloc(8+64+9+ileng+16);
	} else {
		pleng=strlen(cargs->subfolder)+1;
		regbuff = (uint8_t*) malloc(8+64+13+pleng+ileng+16);
	}

	fd = tcpsocket();
	if (fd<0) {
		free(regbuff);
		return -1;
	}
	if (tcpnodelay(fd)<0) {
		if (oninit) {
			fprintf(stderr,"can't set TCP_NODELAY\n");
		} else {
			syslog(LOG_WARNING,"can't set TCP_NODELAY");
		}
	}
	if (srcip>0) {
		if (tcpnumbind(fd,srcip,0)<0) {
			if (oninit) {
				fprintf(stderr,"can't bind socket to given ip (\"%s\")\n",srcstrip);
			} else {
				syslog(LOG_WARNING,"can't bind socket to given ip (\"%s\")",srcstrip);
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
	}
	if (tcpnumconnect(fd,masterip,masterport)<0) {
		if (oninit) {
			fprintf(stderr,"can't connect to mfsmaster (\"%s\":\"%" PRIu16 "\")\n",masterstrip,masterport);
		} else {
			syslog(LOG_WARNING,"can't connect to mfsmaster (\"%s\":\"%" PRIu16 "\")",masterstrip,masterport);
		}
		tcpclose(fd);
		fd=-1;
		free(regbuff);
		return -1;
	}
	if (havepassword) {
		wptr = regbuff;
		put32bit(&wptr,CLTOMA_FUSE_REGISTER);
		put32bit(&wptr,65);
		memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
		wptr+=64;
		put8bit(&wptr,REGISTER_GETRANDOM);
		if (tcptowrite(fd,regbuff,8+65,1000)!=8+65) {
			if (oninit) {
				fprintf(stderr,"error sending data to mfsmaster\n");
			} else {
				syslog(LOG_WARNING,"error sending data to mfsmaster");
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		if (tcptoread(fd,regbuff,8,1000)!=8) {
			if (oninit) {
				fprintf(stderr,"error receiving data from mfsmaster\n");
			} else {
				syslog(LOG_WARNING,"error receiving data from mfsmaster");
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		rptr = regbuff;
		i = get32bit(&rptr);
		if (i!=MATOCL_FUSE_REGISTER) {
			if (oninit) {
				fprintf(stderr,"got incorrect answer from mfsmaster\n");
			} else {
				syslog(LOG_WARNING,"got incorrect answer from mfsmaster");
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		i = get32bit(&rptr);
		if (i!=32) {
			if (oninit) {
				fprintf(stderr,"got incorrect answer from mfsmaster\n");
			} else {
				syslog(LOG_WARNING,"got incorrect answer from mfsmaster");
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		if (tcptoread(fd,regbuff,32,1000)!=32) {
			if (oninit) {
				fprintf(stderr,"error receiving data from mfsmaster\n");
			} else {
				syslog(LOG_WARNING,"error receiving data from mfsmaster");
			}
			tcpclose(fd);
			fd=-1;
			free(regbuff);
			return -1;
		}
		md5_init(&ctx);
		md5_update(&ctx,regbuff,16);
		md5_update(&ctx,cargs->passworddigest,16);
		md5_update(&ctx,regbuff+16,16);
		md5_final(digest,&ctx);
	}
	wptr = regbuff;
	put32bit(&wptr,CLTOMA_FUSE_REGISTER);
	if (cargs->meta) {
		if (havepassword) {
			put32bit(&wptr,64+9+ileng+16);
		} else {
			put32bit(&wptr,64+9+ileng);
		}
	} else {
		if (havepassword) {
			put32bit(&wptr,64+13+ileng+pleng+16);
		} else {
			put32bit(&wptr,64+13+ileng+pleng);
		}
	}
	memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
	wptr+=64;
	put8bit(&wptr,(cargs->meta)?REGISTER_NEWMETASESSION:REGISTER_NEWSESSION);
	put16bit(&wptr,PACKAGE_VERSION_MAJOR);
	put8bit(&wptr,PACKAGE_VERSION_MINOR);
	put8bit(&wptr,PACKAGE_VERSION_MICRO);
	put32bit(&wptr,ileng);
	memcpy(wptr,cargs->info,ileng);
	wptr+=ileng;
	if (!cargs->meta) {
		put32bit(&wptr,pleng);
		memcpy(wptr,cargs->subfolder,pleng);
	}
	if (havepassword) {
		memcpy(wptr+pleng,digest,16);
	}
	if (tcptowrite(fd,regbuff,8+64+(cargs->meta?9:13)+ileng+pleng+(havepassword?16:0),1000)!=(int32_t)(8+64+(cargs->meta?9:13)+ileng+pleng+(havepassword?16:0))) {
		if (oninit) {
			fprintf(stderr,"error sending data to mfsmaster: %s\n",strerr(errno));
		} else {
			syslog(LOG_WARNING,"error sending data to mfsmaster: %s",strerr(errno));
		}
		tcpclose(fd);
		fd=-1;
		free(regbuff);
		return -1;
	}
	if (tcptoread(fd,regbuff,8,1000)!=8) {
		if (oninit) {
			fprintf(stderr,"error receiving data from mfsmaster: %s\n",strerr(errno));
		} else {
			syslog(LOG_WARNING,"error receiving data from mfsmaster: %s",strerr(errno));
		}
		tcpclose(fd);
		fd=-1;
		free(regbuff);
		return -1;
	}
	rptr = regbuff;
	i = get32bit(&rptr);
	if (i!=MATOCL_FUSE_REGISTER) {
		if (oninit) {
			fprintf(stderr,"got incorrect answer from mfsmaster\n");
		} else {
			syslog(LOG_WARNING,"got incorrect answer from mfsmaster");
		}
		tcpclose(fd);
		fd=-1;
		free(regbuff);
		return -1;
	}
	i = get32bit(&rptr);
	if (!(i==1 || (cargs->meta && (i==5 || i==9 || i==19)) || (cargs->meta==0 && (i==13 || i==21 || i==25 || i==35)))) {
		if (oninit) {
			fprintf(stderr,"got incorrect answer from mfsmaster\n");
		} else {
			syslog(LOG_WARNING,"got incorrect answer from mfsmaster");
		}
		tcpclose(fd);
		fd=-1;
		free(regbuff);
		return -1;
	}
	if (tcptoread(fd,regbuff,i,1000)!=(int32_t)i) {
		if (oninit) {
			fprintf(stderr,"error receiving data from mfsmaster: %s\n",strerr(errno));
		} else {
			syslog(LOG_WARNING,"error receiving data from mfsmaster: %s",strerr(errno));
		}
		tcpclose(fd);
		fd=-1;
		free(regbuff);
		return -1;
	}
	rptr = regbuff;
	if (i==1) {
		if (oninit) {
			fprintf(stderr,"mfsmaster register error: %s\n",mfs_strerror(rptr[0]));
		} else {
			syslog(LOG_WARNING,"mfsmaster register error: %s",mfs_strerror(rptr[0]));
		}
		tcpclose(fd);
		fd=-1;
		free(regbuff);
		return -1;
	}
	if (i==9 || i==19 || i==25 || i==35) {
		masterversion = get32bit(&rptr);
	} else {
		masterversion = 0;
	}
	sessionid = get32bit(&rptr);
	sesflags = get8bit(&rptr);
	if (!cargs->meta) {
		rootuid = get32bit(&rptr);
		rootgid = get32bit(&rptr);
		if (i==21) {
			mapalluid = get32bit(&rptr);
			mapallgid = get32bit(&rptr);
		} else {
			mapalluid = 0;
			mapallgid = 0;
		}
	} else {
		rootuid = 0;
		rootgid = 0;
		mapalluid = 0;
		mapallgid = 0;
	}
	if (i==19 || i==35) {
		mingoal = get8bit(&rptr);
		maxgoal = get8bit(&rptr);
		mintrashtime = get32bit(&rptr);
		maxtrashtime = get32bit(&rptr);
	} else {
		mingoal = 0;
		maxgoal = 0;
		mintrashtime = 0;
		maxtrashtime = 0;
	}
	free(regbuff);
	lastwrite=time(NULL);
	if (oninit==0) {
		syslog(LOG_NOTICE,"registered to master with new session");
	}
	if (cargs->clearpassword && cargs->passworddigest!=NULL) {
		memset(cargs->passworddigest,0,16);
		free(cargs->passworddigest);
		cargs->passworddigest = NULL;
	}
	if (oninit==1) {
		fprintf(stderr,"mfsmaster accepted connection with parameters: ");
		j=0;
		for (i=0 ; i<8 ; i++) {
			if (sesflags&(1<<i)) {
				fprintf(stderr,"%s%s",j?",":"",sesflagposstrtab[i]);
				j=1;
			} else if (sesflagnegstrtab[i]) {
				fprintf(stderr,"%s%s",j?",":"",sesflagnegstrtab[i]);
				j=1;
			}
		}
		if (j==0) {
			fprintf(stderr,"-");
		}
		if (!cargs->meta) {
			fprintf(stderr," ; root mapped to ");
			getpwuid_r(rootuid,&pwd,pwdgrpbuff,16384,&pw);
			if (pw) {
				fprintf(stderr,"%s:",pw->pw_name);
			} else {
				fprintf(stderr,"%" PRIu32 ":",rootuid);
			}
			getgrgid_r(rootgid,&grp,pwdgrpbuff,16384,&gr);
			if (gr) {
				fprintf(stderr,"%s",gr->gr_name);
			} else {
				fprintf(stderr,"%" PRIu32,rootgid);
			}
			if (sesflags&SESFLAG_MAPALL) {
				fprintf(stderr," ; users mapped to ");
				pw = getpwuid(mapalluid);
				if (pw) {
					fprintf(stderr,"%s:",pw->pw_name);
				} else {
					fprintf(stderr,"%" PRIu32 ":",mapalluid);
				}
				gr = getgrgid(mapallgid);
				if (gr) {
					fprintf(stderr,"%s",gr->gr_name);
				} else {
					fprintf(stderr,"%" PRIu32,mapallgid);
				}
			}
		}
		if (mingoal>0 && maxgoal>0) {
			if (mingoal>1 || maxgoal<9) {
				fprintf(stderr," ; setgoal limited to (%u:%u)",mingoal,maxgoal);
			}
			if (mintrashtime>0 || maxtrashtime<UINT32_C(0xFFFFFFFF)) {
				fprintf(stderr," ; settrashtime limited to (");
				if (mintrashtime>0) {
					if (mintrashtime>604800) {
						fprintf(stderr,"%uw",mintrashtime/604800);
						mintrashtime %= 604800;
					}
					if (mintrashtime>86400) {
						fprintf(stderr,"%ud",mintrashtime/86400);
						mintrashtime %= 86400;
					}
					if (mintrashtime>3600) {
						fprintf(stderr,"%uh",mintrashtime/3600);
						mintrashtime %= 3600;
					}
					if (mintrashtime>60) {
						fprintf(stderr,"%um",mintrashtime/60);
						mintrashtime %= 60;
					}
					if (mintrashtime>0) {
						fprintf(stderr,"%us",mintrashtime);
					}
				} else {
					fprintf(stderr,"0s");
				}
				fprintf(stderr,":");
				if (maxtrashtime>0) {
					if (maxtrashtime>604800) {
						fprintf(stderr,"%uw",maxtrashtime/604800);
						maxtrashtime %= 604800;
					}
					if (maxtrashtime>86400) {
						fprintf(stderr,"%ud",maxtrashtime/86400);
						maxtrashtime %= 86400;
					}
					if (maxtrashtime>3600) {
						fprintf(stderr,"%uh",maxtrashtime/3600);
						maxtrashtime %= 3600;
					}
					if (maxtrashtime>60) {
						fprintf(stderr,"%um",maxtrashtime/60);
						maxtrashtime %= 60;
					}
					if (maxtrashtime>0) {
						fprintf(stderr,"%us",maxtrashtime);
					}
				} else {
					fprintf(stderr,"0s");
				}
				fprintf(stderr,")");
			}
		}
		fprintf(stderr,"\n");
	}
	return 0;
}

void fs_reconnect() {
	uint32_t i;
	uint8_t *wptr,regbuff[8+64+9];
	const uint8_t *rptr;

	if (sessionid==0) {
		syslog(LOG_WARNING,"can't register: session not created");
		return;
	}

	fd = tcpsocket();
	if (fd<0) {
		return;
	}
	if (tcpnodelay(fd)<0) {
		syslog(LOG_WARNING,"can't set TCP_NODELAY: %s",strerr(errno));
	}
	if (srcip>0) {
		if (tcpnumbind(fd,srcip,0)<0) {
			syslog(LOG_WARNING,"can't bind socket to given ip (\"%s\")",srcstrip);
			tcpclose(fd);
			fd=-1;
			return;
		}
	}
	if (tcpnumconnect(fd,masterip,masterport)<0) {
		syslog(LOG_WARNING,"can't connect to master (\"%s\":\"%" PRIu16 "\")",masterstrip,masterport);
		tcpclose(fd);
		fd=-1;
		return;
	}
	master_stats_inc(MASTER_CONNECTS);
	wptr = regbuff;
	put32bit(&wptr,CLTOMA_FUSE_REGISTER);
	put32bit(&wptr,73);
	memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
	wptr+=64;
	put8bit(&wptr,REGISTER_RECONNECT);
	put32bit(&wptr,sessionid);
	put16bit(&wptr,PACKAGE_VERSION_MAJOR);
	put8bit(&wptr,PACKAGE_VERSION_MINOR);
	put8bit(&wptr,PACKAGE_VERSION_MICRO);
	if (tcptowrite(fd,regbuff,8+64+9,1000)!=8+64+9) {
		syslog(LOG_WARNING,"master: register error (write: %s)",strerr(errno));
		tcpclose(fd);
		fd=-1;
		return;
	}
	master_stats_add(MASTER_BYTESSENT,16+64);
	master_stats_inc(MASTER_PACKETSSENT);
	if (tcptoread(fd,regbuff,8,1000)!=8) {
		syslog(LOG_WARNING,"master: register error (read header: %s)",strerr(errno));
		tcpclose(fd);
		fd=-1;
		return;
	}
	master_stats_add(MASTER_BYTESRCVD,8);
	rptr = regbuff;
	i = get32bit(&rptr);
	if (i!=MATOCL_FUSE_REGISTER) {
		syslog(LOG_WARNING,"master: register error (bad answer: %" PRIu32 ")",i);
		tcpclose(fd);
		fd=-1;
		return;
	}
	i = get32bit(&rptr);
	if (i!=1) {
		syslog(LOG_WARNING,"master: register error (bad length: %" PRIu32 ")",i);
		tcpclose(fd);
		fd=-1;
		return;
	}
	if (tcptoread(fd,regbuff,i,1000)!=(int32_t)i) {
		syslog(LOG_WARNING,"master: register error (read data: %s)",strerr(errno));
		tcpclose(fd);
		fd=-1;
		return;
	}
	master_stats_add(MASTER_BYTESRCVD,i);
	master_stats_inc(MASTER_PACKETSRCVD);
	rptr = regbuff;
	if (rptr[0]!=0) {
		sessionlost=1;
		syslog(LOG_WARNING,"master: register status: %s",mfs_strerror(rptr[0]));
		tcpclose(fd);
		fd=-1;
		return;
	}
	lastwrite=time(NULL);
	syslog(LOG_NOTICE,"registered to master");
}

void fs_close_session(void) {
	uint8_t *wptr,regbuff[8+64+5];

	if (sessionid==0) {
		return;
	}

	wptr = regbuff;
	put32bit(&wptr,CLTOMA_FUSE_REGISTER);
	put32bit(&wptr,69);
	memcpy(wptr,FUSE_REGISTER_BLOB_ACL,64);
	wptr+=64;
	put8bit(&wptr,REGISTER_CLOSESESSION);
	put32bit(&wptr,sessionid);
	if (tcptowrite(fd,regbuff,8+64+5,1000)!=8+64+5) {
		syslog(LOG_WARNING,"master: close session error (write: %s)",strerr(errno));
	}
}

void* fs_nop_thread(void *arg) {
	uint8_t *ptr,hdr[12],*inodespacket;
	int32_t inodesleng;
	acquired_file *afptr;
	int now;
	int inodeswritecnt=0;
	(void)arg;
	for (;;) {
		now = time(NULL);
		std::unique_lock<std::mutex> fdLock(fdMutex);
		if (fterm) {
			if (fd>=0) {
				fs_close_session();
			}
			return NULL;
		}
		if (disconnect == false && fd >= 0) {
			if (lastwrite+2<now) {	// NOP
				ptr = hdr;
				put32bit(&ptr,ANTOAN_NOP);
				put32bit(&ptr,4);
				put32bit(&ptr,0);
				if (tcptowrite(fd,hdr,12,1000)!=12) {
					disconnect = true;
				} else {
					master_stats_add(MASTER_BYTESSENT,12);
					master_stats_inc(MASTER_PACKETSSENT);
				}
				lastwrite=now;
			}
			if (inodeswritecnt<=0 || inodeswritecnt>60) {
				inodeswritecnt=60;
			} else {
				inodeswritecnt--;
			}
			if (inodeswritecnt==0) {	// HELD INODES
				std::unique_lock<std::mutex> asLock(acquiredFileMutex);
				inodesleng=8;
				for (afptr=afhead ; afptr ; afptr=afptr->next) {
					//syslog(LOG_NOTICE,"reserved inode: %" PRIu32,afptr->inode);
					inodesleng+=4;
				}
				inodespacket = (uint8_t*) malloc(inodesleng);
				ptr = inodespacket;
				put32bit(&ptr,CLTOMA_FUSE_RESERVED_INODES);
				put32bit(&ptr,inodesleng-8);
				for (afptr=afhead ; afptr ; afptr=afptr->next) {
					put32bit(&ptr,afptr->inode);
				}
				if (tcptowrite(fd,inodespacket,inodesleng,1000)!=inodesleng) {
					disconnect = true;
				} else {
					master_stats_add(MASTER_BYTESSENT,inodesleng);
					master_stats_inc(MASTER_PACKETSSENT);
				}
				free(inodespacket);
			}
		}
		fdLock.unlock();
		sleep(1);
	}
}

template<class... Args>
bool fsTryReadFromMaster(uint32_t& messageSize, Args&... destination) {
	std::vector<uint8_t> buffer(serializedSize(destination...));
	if (messageSize < buffer.size()) {
		syslog(LOG_WARNING,"master: packet too small");
		setDisconnect(true);
		return false;
	}
	int bytesRead = tcptoread(fd, buffer.data(), buffer.size(), RECEIVE_TIMEOUT * 1000);
	if (bytesRead == 0) {
		syslog(LOG_WARNING, "master: connection lost (1)");
		disconnect = true;
		return false;
	} else if (bytesRead != static_cast<int>(buffer.size())) {
		syslog(LOG_WARNING, "master: tcp recv error: %s (1)", strerr(errno));
		setDisconnect(true);
		return false;
	}
	try {
		deserialize(buffer, destination...);
	} catch (IncorrectDeserializationException& e) {
		syslog(LOG_WARNING,"master: tcp recv error: %s", e.what());
		setDisconnect(true);
		return false;
	}
	master_stats_add(MASTER_BYTESRCVD, buffer.size());
	messageSize -= buffer.size();
	return true;
}

void* fs_receive_thread(void *arg) {
	threc *rec;
	int r;

	(void)arg;
	for (;;) {
		std::unique_lock<std::mutex>fdLock(fdMutex);
		if (fterm) {
			return NULL;
		}
		if (disconnect) {
			tcpclose(fd);
			fd=-1;
			disconnect = false;
			// send to any threc status error and unlock them
			std::unique_lock<std::mutex>recLock(recMutex);
			for (rec=threchead ; rec ; rec=rec->next) {
				std::unique_lock<std::mutex> lock(rec->mutex);
				if (rec->sent) {
					rec->status = 1;
					rec->rcvd = 1;
					if (rec->waiting) {
						rec->condition.notify_one();
					}
				}
			}
		}
		if (fd==-1 && sessionid!=0) {
			fs_reconnect();		// try to register using the same session id
		}
		if (fd==-1) {	// still not connected
			if (sessionlost) {	// if previous session is lost then try to register as a new session
				if (fs_connect(0,&connect_args)==0) {
					sessionlost=0;
				}
			} else {	// if other problem occured then try to resolve hostname and portname then try to reconnect using the same session id
				if (fs_resolve(0,connect_args.bindhostname,connect_args.masterhostname,connect_args.masterportname)==0) {
					fs_reconnect();
				}
			}
		}
		if (fd==-1) {
			fdLock.unlock();
			sleep(2);	// reconnect every 2 seconds
			continue;
		}
		fdLock.unlock();

		PacketHeader packetHeader;
		PacketVersion packetVersion;
		uint32_t messageId;
		uint32_t messageSize = serializedSize(packetHeader);
		uint32_t responseSize; // size of the buffer passed to the receiver
		if (!fsTryReadFromMaster(messageSize, packetHeader)) {
			continue;
		}
		master_stats_inc(MASTER_PACKETSRCVD);
		messageSize = packetHeader.length;

		if (packetHeader.isLizPacketType()) {
			if (!fsTryReadFromMaster(messageSize, packetVersion, messageId)) {
				continue;
			}
			responseSize = packetHeader.length;
		} else {
			if (!fsTryReadFromMaster(messageSize, messageId)) {
				continue;
			}
			responseSize = packetHeader.length - 4; // we will strip out messageId
		}

		if (messageId == 0) {
			if (packetHeader.type == ANTOAN_NOP && messageSize == 0) {
				continue;
			}
			if (packetHeader.type == ANTOAN_UNKNOWN_COMMAND ||
					packetHeader.type == ANTOAN_BAD_COMMAND_SIZE) { // just ignore these packets with packetid==0
				continue;
			}
		}
		rec = fs_get_threc_by_id(messageId);
		if (rec == NULL) {
			syslog(LOG_WARNING,"master: got unexpected queryid");
			setDisconnect(true);
			continue;
		}
		std::unique_lock<std::mutex> lock(rec->mutex);
		if (rec->inputBuffer.size() != responseSize) {
			rec->inputBuffer.resize(responseSize);
		}
		uint8_t *responsePointer = rec->inputBuffer.data();
		if (packetHeader.isLizPacketType()) {
			rec->inputBuffer.resize(0);	// make serialize() happy
			serialize(rec->inputBuffer, packetVersion, messageId);
			rec->inputBuffer.resize(responseSize);
			responsePointer = rec->inputBuffer.data()
				+ serializedSize(packetVersion, messageId);
		}
		// syslog(LOG_NOTICE,"master: expected data size: %" PRIu32,size);
		if (messageSize > 0) {
			r = tcptoread(fd, responsePointer, messageSize, 1000);
			// syslog(LOG_NOTICE,"master: data size: %d",r);
			if (r == 0) {
				syslog(LOG_WARNING,"master: connection lost (2)");
				lock.unlock();
				setDisconnect(true);
				continue;
			}
			if (r != (int)messageSize) {
				syslog(LOG_WARNING,"master: tcp recv error: %s (2)",strerr(errno));
				lock.unlock();
				setDisconnect(true);
				continue;
			}
			master_stats_add(MASTER_BYTESRCVD, messageSize);
		}
		rec->sent = 0;
		rec->status = 0;
		rec->idataleng = responseSize;
		rec->rcvd_cmd = packetHeader.type;
		// syslog(LOG_NOTICE,"master: unlock: %" PRIu32,rec->packetid);
		rec->rcvd = 1;
		if (rec->waiting) {
			rec->condition.notify_one();
		}
	}
}

// called before fork
int fs_init_master_connection(const char *bindhostname,const char *masterhostname,const char *masterportname,uint8_t meta,const char *info,const char *subfolder,const uint8_t passworddigest[16],uint8_t donotrememberpassword,uint8_t bgregister) {
	master_statsptr_init();

	fd = -1;
	sessionlost = bgregister;
	sessionid = 0;
	disconnect = false;

	if (bindhostname) {
		connect_args.bindhostname = strdup(bindhostname);
	} else {
		connect_args.bindhostname = NULL;
	}
	connect_args.masterhostname = strdup(masterhostname);
	connect_args.masterportname = strdup(masterportname);
	connect_args.meta = meta;
	connect_args.clearpassword = donotrememberpassword;
	connect_args.info = strdup(info);
	connect_args.subfolder = strdup(subfolder);
	if (passworddigest==NULL) {
		connect_args.passworddigest = NULL;
	} else {
		connect_args.passworddigest = (uint8_t*) malloc(16);
		memcpy(connect_args.passworddigest,passworddigest,16);
	}

	if (bgregister) {
		return 1;
	}
	return fs_connect(1,&connect_args);
}

// called after fork
void fs_init_threads(uint32_t retries) {
	pthread_attr_t thattr;
	maxretries = retries;
	fterm = 0;

	pthread_attr_init(&thattr);
	pthread_attr_setstacksize(&thattr,0x100000);
	pthread_create(&rpthid,&thattr,fs_receive_thread,NULL);
	pthread_create(&npthid,&thattr,fs_nop_thread,NULL);
	pthread_attr_destroy(&thattr);
}

void fs_term(void) {
	threc *tr,*trn;
	acquired_file *af,*afn;
	std::unique_lock<std::mutex> fdLock(fdMutex);
	fterm = 1;
	fdLock.unlock();
	pthread_join(npthid,NULL);
	pthread_join(rpthid,NULL);
	for (tr = threchead ; tr ; tr = trn) {
		trn = tr->next;
		if (tr->obuff) {
#ifdef MMAP_ALLOC
			munmap((void*)(tr->obuff),tr->obuffsize);
#else
			free(tr->obuff);
#endif
		}
		tr->inputBuffer.clear();
		delete tr;
	}
	for (af = afhead ; af ; af = afn) {
		afn = af->next;
		free(af);
	}
	if (fd>=0) {
		tcpclose(fd);
	}
	if (connect_args.bindhostname) {
		free(connect_args.bindhostname);
	}
	free(connect_args.masterhostname);
	free(connect_args.masterportname);
	free(connect_args.info);
	free(connect_args.subfolder);
	if (connect_args.passworddigest) {
		free(connect_args.passworddigest);
	}
}

void fs_statfs(uint64_t *totalspace,uint64_t *availspace,uint64_t *trashspace,uint64_t *reservedspace,uint32_t *inodes) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_STATFS,0);
	if (wptr==NULL) {
		*totalspace = 0;
		*availspace = 0;
		*trashspace = 0;
		*reservedspace = 0;
		*inodes = 0;
		return;
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_STATFS,&i);
	if (rptr==NULL || i!=36) {
		*totalspace = 0;
		*availspace = 0;
		*trashspace = 0;
		*reservedspace = 0;
		*inodes = 0;
	} else {
		*totalspace = get64bit(&rptr);
		*availspace = get64bit(&rptr);
		*trashspace = get64bit(&rptr);
		*reservedspace = get64bit(&rptr);
		*inodes = get32bit(&rptr);
	}
}

uint8_t fs_access(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t modemask) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_ACCESS,13);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,modemask);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_ACCESS,&i);
	if (!rptr || i!=1) {
		ret = ERROR_IO;
	} else {
		ret = rptr[0];
	}
	return ret;
}

uint8_t fs_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_LOOKUP,13+nleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_LOOKUP,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		t32 = get32bit(&rptr);
		*inode = t32;
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getattr(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETATTR,12);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_setattr(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t setmask,uint16_t attrmode,uint32_t attruid,uint32_t attrgid,uint32_t attratime,uint32_t attrmtime,uint8_t sugidclearmode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (masterversion<0x010619) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETATTR,31);
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_SETATTR,32);
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,setmask);
	put16bit(&wptr,attrmode);
	put32bit(&wptr,attruid);
	put32bit(&wptr,attrgid);
	put32bit(&wptr,attratime);
	put32bit(&wptr,attrmtime);
	if (masterversion>=0x010619) {
		put8bit(&wptr,sugidclearmode);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_truncate(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint64_t attrlength,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_TRUNCATE,21);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put8bit(&wptr,opened);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put64bit(&wptr,attrlength);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_TRUNCATE,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_readlink(uint32_t inode,const uint8_t **path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_READLINK,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_READLINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		pleng = get32bit(&rptr);
		if (i!=4+pleng || pleng==0 || rptr[pleng-1]!=0) {
			setDisconnect(true);
			ret = ERROR_IO;
		} else {
			*path = rptr;
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_symlink(uint32_t parent,uint8_t nleng,const uint8_t *name,const uint8_t *path,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	t32 = strlen((const char *)path)+1;
	wptr = fs_createpacket(rec,CLTOMA_FUSE_SYMLINK,t32+nleng+17);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,t32);
	memcpy(wptr,path,t32);
	wptr+=t32;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SYMLINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		t32 = get32bit(&rptr);
		*inode = t32;
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_mknod(uint32_t parent,uint8_t nleng,const uint8_t *name,uint8_t type,uint16_t mode,uint32_t uid,uint32_t gid,uint32_t rdev,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_MKNOD,20+nleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put8bit(&wptr,type);
	put16bit(&wptr,mode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put32bit(&wptr,rdev);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_MKNOD,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		t32 = get32bit(&rptr);
		*inode = t32;
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_mkdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint16_t mode,uint32_t uid,uint32_t gid,uint8_t copysgid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (masterversion<0x010619) {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKDIR,15+nleng);
	} else {
		wptr = fs_createpacket(rec,CLTOMA_FUSE_MKDIR,16+nleng);
	}
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put16bit(&wptr,mode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	if (masterversion>=0x010619) {
		put8bit(&wptr,copysgid);
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_MKDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		t32 = get32bit(&rptr);
		*inode = t32;
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_unlink(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_UNLINK,13+nleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_UNLINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		setDisconnect(true);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_rmdir(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t uid,uint32_t gid) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_RMDIR,13+nleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_RMDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		setDisconnect(true);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_rename(uint32_t parent_src,uint8_t nleng_src,const uint8_t *name_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_RENAME,18+nleng_src+nleng_dst);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,parent_src);
	put8bit(&wptr,nleng_src);
	memcpy(wptr,name_src,nleng_src);
	wptr+=nleng_src;
	put32bit(&wptr,parent_dst);
	put8bit(&wptr,nleng_dst);
	memcpy(wptr,name_dst,nleng_dst);
	wptr+=nleng_dst;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_RENAME,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
		*inode = 0;
		memset(attr,0,35);
	} else if (i!=39) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		t32 = get32bit(&rptr);
		*inode = t32;
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_link(uint32_t inode_src,uint32_t parent_dst,uint8_t nleng_dst,const uint8_t *name_dst,uint32_t uid,uint32_t gid,uint32_t *inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_LINK,17+nleng_dst);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode_src);
	put32bit(&wptr,parent_dst);
	put8bit(&wptr,nleng_dst);
	memcpy(wptr,name_dst,nleng_dst);
	wptr+=nleng_dst;
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_LINK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=39) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		t32 = get32bit(&rptr);
		*inode = t32;
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getdir(uint32_t inode,uint32_t uid,uint32_t gid,const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETDIR,12);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getdir_plus(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t addtocache,const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint8_t flags;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETDIR,13);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	flags = GETDIR_FLAG_WITHATTR;
	if (addtocache) {
		flags |= GETDIR_FLAG_ADDTOCACHE;
	}
	put8bit(&wptr,flags);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETDIR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

// FUSE - I/O

uint8_t fs_opencheck(uint32_t inode,uint32_t uid,uint32_t gid,uint8_t flags,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_OPEN,13);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,flags);
	fs_inc_acnt(inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_OPEN,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		if (attr) {
			memset(attr,0,35);
		}
		ret = rptr[0];
	} else if (i==35) {
		if (attr) {
			memcpy(attr,rptr,35);
		}
		ret = STATUS_OK;
	} else {
		setDisconnect(true);
		ret = ERROR_IO;
	}
	if (ret) {	// release on error
		fs_dec_acnt(inode);
	}
	return ret;
}

void fs_release(uint32_t inode) {
	fs_dec_acnt(inode);
}

uint8_t fs_readchunk(uint32_t inode,uint32_t indx,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint64_t t64;
	uint32_t t32;
	threc *rec = fs_get_my_threc();
	*csdata=NULL;
	*csdatasize=0;
	wptr = fs_createpacket(rec,CLTOMA_FUSE_READ_CHUNK,8);
	if (wptr==NULL) {
		return ERROR_IO;
	}

	put32bit(&wptr,inode);
	put32bit(&wptr,indx);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_READ_CHUNK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<20 || ((i-20)%6)!=0) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		t64 = get64bit(&rptr);
		*length = t64;
		t64 = get64bit(&rptr);
		*chunkid = t64;
		t32 = get32bit(&rptr);
		*version = t32;
		if (i>20) {
			*csdata = rptr;
			*csdatasize = i-20;
		}
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fsLizReadChunk(std::vector<ChunkTypeWithAddress>& serverList, uint64_t& chunkId,
		uint32_t& chunkVersion, uint64_t& fileLength, uint32_t inode, uint32_t chunkIndex) {
	threc *rec = fs_get_my_threc();

	std::vector<uint8_t> message;
	cltoma::fuseReadChunk::serialize(message, rec->packetid, inode, chunkIndex);
	if (!fsLizCreatePacket(rec, message)) {
		return ERROR_IO;
	}

	try {
		if (!fsLizSendAndReceive(rec, LIZ_MATOCL_FUSE_READ_CHUNK, message)) {
			return ERROR_IO;
		}
		PacketVersion packetVersion;
		deserializePacketVersionNoHeader(message, packetVersion);

		if (packetVersion == matocl::fuseReadChunk::kStatusPacketVersion) {
			uint8_t status;
			matocl::fuseReadChunk::deserialize(message, status);
			return status;
		} else if (packetVersion == matocl::fuseReadChunk::kResponsePacketVersion) {
			matocl::fuseReadChunk::deserialize(message, fileLength, chunkId, chunkVersion, serverList);
		} else {
			syslog(LOG_NOTICE,"LIZ_MATOCL_FUSE_READ_CHUNK - wrong packet version");
		}
	} catch (IncorrectDeserializationException&) {
		setDisconnect(true);
		return ERROR_IO;
	}
	return STATUS_OK;
}

uint8_t fs_writechunk(uint32_t inode,uint32_t indx,uint64_t *length,uint64_t *chunkid,uint32_t *version,const uint8_t **csdata,uint32_t *csdatasize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	uint64_t t64;
	uint32_t t32;
	threc *rec = fs_get_my_threc();
	*csdata=NULL;
	*csdatasize=0;
	wptr = fs_createpacket(rec,CLTOMA_FUSE_WRITE_CHUNK,8);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,indx);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_WRITE_CHUNK,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<20 || ((i-20)%6)!=0) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		t64 = get64bit(&rptr);
		*length = t64;
		t64 = get64bit(&rptr);
		*chunkid = t64;
		t32 = get32bit(&rptr);
		*version = t32;
		if (i>20) {
			*csdata = rptr;
			*csdatasize = i-20;
		}
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_writeend(uint64_t chunkid, uint32_t inode, uint64_t length) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_WRITE_CHUNK_END,20);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put64bit(&wptr,chunkid);
	put32bit(&wptr,inode);
	put64bit(&wptr,length);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_WRITE_CHUNK_END,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		setDisconnect(true);
		ret = ERROR_IO;
	}
	return ret;
}


// FUSE - META


uint8_t fs_getreserved(const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETRESERVED,0);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETRESERVED,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_gettrash(const uint8_t **dbuff,uint32_t *dbuffsize) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETTRASH,0);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETTRASH,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		*dbuff = rptr;
		*dbuffsize = i;
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_getdetachedattr(uint32_t inode,uint8_t attr[35]) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETDETACHEDATTR,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETDETACHEDATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i!=35) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		memcpy(attr,rptr,35);
		ret = STATUS_OK;
	}
	return ret;
}

uint8_t fs_gettrashpath(uint32_t inode,const uint8_t **path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t pleng;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETTRASHPATH,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETTRASHPATH,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		pleng = get32bit(&rptr);
		if (i!=4+pleng || pleng==0 || rptr[pleng-1]!=0) {
			setDisconnect(true);
			ret = ERROR_IO;
		} else {
			*path = rptr;
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_settrashpath(uint32_t inode,const uint8_t *path) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint32_t t32;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	t32 = strlen((const char *)path)+1;
	wptr = fs_createpacket(rec,CLTOMA_FUSE_SETTRASHPATH,t32+8);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put32bit(&wptr,t32);
	memcpy(wptr,path,t32);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETTRASHPATH,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		setDisconnect(true);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_undel(uint32_t inode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_UNDEL,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_UNDEL,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		setDisconnect(true);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_purge(uint32_t inode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,CLTOMA_FUSE_PURGE,4);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_PURGE,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		setDisconnect(true);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_getxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t nleng,const uint8_t *name,uint8_t mode,const uint8_t **vbuff,uint32_t *vleng) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (masterversion<0x010700) {
		return ERROR_ENOTSUP;
	}
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETXATTR,15+nleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put8bit(&wptr,opened);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put8bit(&wptr,mode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETXATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		*vleng = get32bit(&rptr);
		*vbuff = (mode==MFS_XATTR_GETA_DATA)?rptr:NULL;
		if ((mode==MFS_XATTR_GETA_DATA && i!=(*vleng)+4) || (mode==MFS_XATTR_LENGTH_ONLY && i!=4)) {
			setDisconnect(true);
			ret = ERROR_IO;
		} else {
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_listxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t mode,const uint8_t **dbuff,uint32_t *dleng) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (masterversion<0x010700) {
		return ERROR_ENOTSUP;
	}
	wptr = fs_createpacket(rec,CLTOMA_FUSE_GETXATTR,15);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put8bit(&wptr,opened);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,0);
	put8bit(&wptr,mode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_GETXATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else if (i<4) {
		setDisconnect(true);
		ret = ERROR_IO;
	} else {
		*dleng = get32bit(&rptr);
		*dbuff = (mode==MFS_XATTR_GETA_DATA)?rptr:NULL;
		if ((mode==MFS_XATTR_GETA_DATA && i!=(*dleng)+4) || (mode==MFS_XATTR_LENGTH_ONLY && i!=4)) {
			setDisconnect(true);
			ret = ERROR_IO;
		} else {
			ret = STATUS_OK;
		}
	}
	return ret;
}

uint8_t fs_setxattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t nleng,const uint8_t *name,uint32_t vleng,const uint8_t *value,uint8_t mode) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (masterversion<0x010700) {
		return ERROR_ENOTSUP;
	}
	if (mode>=MFS_XATTR_REMOVE) {
		return ERROR_EINVAL;
	}
	wptr = fs_createpacket(rec,CLTOMA_FUSE_SETXATTR,19+nleng+vleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put8bit(&wptr,opened);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,vleng);
	memcpy(wptr,value,vleng);
	wptr+=vleng;
	put8bit(&wptr,mode);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETXATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		setDisconnect(true);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_removexattr(uint32_t inode,uint8_t opened,uint32_t uid,uint32_t gid,uint8_t nleng,const uint8_t *name) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	if (masterversion<0x010700) {
		return ERROR_ENOTSUP;
	}
	wptr = fs_createpacket(rec,CLTOMA_FUSE_SETXATTR,19+nleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	put32bit(&wptr,inode);
	put8bit(&wptr,opened);
	put32bit(&wptr,uid);
	put32bit(&wptr,gid);
	put8bit(&wptr,nleng);
	memcpy(wptr,name,nleng);
	wptr+=nleng;
	put32bit(&wptr,0);
	put8bit(&wptr,MFS_XATTR_REMOVE);
	rptr = fs_sendandreceive(rec,MATOCL_FUSE_SETXATTR,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else if (i==1) {
		ret = rptr[0];
	} else {
		setDisconnect(true);
		ret = ERROR_IO;
	}
	return ret;
}

uint8_t fs_custom(uint32_t qcmd,const uint8_t *query,uint32_t queryleng,uint32_t *acmd,uint8_t *answer,uint32_t *answerleng) {
	uint8_t *wptr;
	const uint8_t *rptr;
	uint32_t i;
	uint8_t ret;
	threc *rec = fs_get_my_threc();
	wptr = fs_createpacket(rec,qcmd,queryleng);
	if (wptr==NULL) {
		return ERROR_IO;
	}
	memcpy(wptr,query,queryleng);
	rptr = fs_sendandreceive_any(rec,acmd,&i);
	if (rptr==NULL) {
		ret = ERROR_IO;
	} else {
		if (*answerleng<i) {
			ret = ERROR_EINVAL;
		} else {
			*answerleng = i;
			memcpy(answer,rptr,i);
			ret = STATUS_OK;
		}
	}
	return ret;
}
