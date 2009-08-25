#include "config.h"

#include <sys/types.h>
#ifdef HAVE_WRITEV
#include <sys/uio.h>
#endif
#include <sys/time.h>
#include <unistd.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>
#include <errno.h>
#include <pthread.h>
#include <inttypes.h>

#include "datapack.h"
#include "crc.h"
#include "th_queue.h"
#include "sockets.h"
#include "mastercomm.h"
#include "readdata.h"
#include "MFSCommunication.h"

#ifndef EDQUOT
#define EDQUOT ENOSPC
#endif

#define WORKERS 10
#define MAXRETRIES 30

#define WCHASHSIZE 256
#define WCHASH(inode,indx) (((inode)*0xB239FB71+(indx)*193)%WCHASHSIZE)

#define IDHASHSIZE 256
#define IDHASH(inode) (((inode)*0xB239FB71)%IDHASHSIZE)

typedef struct cblock_s {
	uint8_t data[65536];	// modified only when writeid==0
	uint16_t pos;		// block in chunk (0...1023) - never modified
	uint32_t writeid;	// 0 = not sent, >0 = block was sent (modified and accessed only when wchunk is locked)
	uint32_t from;		// first filled byte in data (modified only when writeid==0)
	uint32_t to;		// first not used byte in data (modified only when writeid==0)
	struct cblock_s *next,*prev;
} cblock;

typedef struct inodedata_s {
	uint32_t inode;
	uint64_t maxfleng;
	uint32_t cacheblocks;
	int status;
	uint16_t flushwaiting;
	uint16_t writewaiting;
	uint16_t cachewaiting;
	uint16_t lcnt;
	uint16_t jcnt;
	pthread_cond_t flushcond;	// wait for jcnt==0 (flush)
	pthread_cond_t writecond;	// wait for flushwaiting==0 (write)
	pthread_cond_t cachecond;	// wait for cache blocks
	struct inodedata_s *next;
} inodedata;

typedef struct wchunk_s {
	uint32_t inode;
	uint16_t chindx;
	uint8_t trycnt;
	uint8_t waitingworker;
	int pipe[2];
	cblock *datachainhead,*datachaintail;
	inodedata *id;
//	pthread_mutex_t lock;
	struct wchunk_s *next;
} wchunk;

// static pthread_mutex_t fcblock;

static pthread_cond_t fcbcond;
static uint8_t fcbwaiting;
static cblock *freecblockshead;
static uint32_t usedblocks;
static uint32_t maxinodecacheblocks;

static inodedata **idhash;
// static pthread_mutex_t idhashlock;

static wchunk **wchash;
// static pthread_mutex_t wchashlock;

static pthread_mutex_t glock;

// static pthread_t info_worker_th;
static pthread_t dqueue_worker_th;
static pthread_t write_worker_th[WORKERS];

static void *jqueue,*dqueue;

#define TIMEDIFF(tv1,tv2) (((int64_t)((tv1).tv_sec-(tv2).tv_sec))*1000000LL+(int64_t)((tv1).tv_usec-(tv2).tv_usec))

/*
void* write_info_worker(void *arg) {
	(void)arg;
	for (;;) {
		pthread_mutex_lock(&glock);
		syslog(LOG_NOTICE,"used cache blocks: %"PRIu32,usedblocks);
		pthread_mutex_unlock(&glock);
		usleep(500000);
	}

}
*/

/* glock: LOCKED */
void write_cb_release (cblock *cb) {
//	pthread_mutex_lock(&fcblock);
	cb->next = freecblockshead;
	freecblockshead = cb;
	if (fcbwaiting) {
		pthread_cond_signal(&fcbcond);
	}
	usedblocks--;
//	pthread_mutex_unlock(&fcblock);
}

/* glock: LOCKED */
cblock* write_cb_acquire(uint8_t *waited) {
	cblock *ret;
//	pthread_mutex_lock(&fcblock);
	fcbwaiting++;
	while (freecblockshead==NULL) {
		*waited=1;
		pthread_cond_wait(&fcbcond,&glock);
	}
	fcbwaiting--;
	ret = freecblockshead;
	freecblockshead = ret->next;
	ret->pos = 0;
	ret->writeid = 0;
	ret->from = 0;
	ret->to = 0;
	ret->next = NULL;
	ret->prev = NULL;
	usedblocks++;
//	pthread_mutex_unlock(&fcblock);
	return ret;
}


/* inode */

/* glock: LOCKED */
inodedata* write_find_inodedata(uint32_t inode) {
	uint32_t idh = IDHASH(inode);
	inodedata *id;
	for (id=idhash[idh] ; id ; id=id->next) {
		if (id->inode == inode) {
			return id;
		}
	}
	return NULL;
}

/* glock: LOCKED */
inodedata* write_get_inodedata(uint32_t inode) {
	uint32_t idh = IDHASH(inode);
	inodedata *id;
	for (id=idhash[idh] ; id ; id=id->next) {
		if (id->inode == inode) {
			return id;
		}
	}
	id = malloc(sizeof(inodedata));
	id->inode = inode;
	id->cacheblocks = 0;
	id->maxfleng = 0;
	id->status = 0;
	id->flushwaiting = 0;
	id->writewaiting = 0;
	id->cachewaiting = 0;
	id->lcnt = 0;
	id->jcnt = 0;
	pthread_cond_init(&(id->flushcond),NULL);
	pthread_cond_init(&(id->writecond),NULL);
	pthread_cond_init(&(id->cachecond),NULL);
	id->next = idhash[idh];
	idhash[idh] = id;
	return id;
}

/* glock: LOCKED */
void write_free_inodedata(inodedata *fid) {
	uint32_t idh = IDHASH(fid->inode);
	inodedata *id,**idp;
	idp = &(idhash[idh]);
	while ((id=*idp)) {
		if (id==fid) {
			*idp = id->next;
			pthread_cond_destroy(&(id->flushcond));
			pthread_cond_destroy(&(id->writecond));
			pthread_cond_destroy(&(id->cachecond));
			free(id);
			return;
		}
		idp = &(id->next);
	}
}


/* queues */

/* glock: UNUSED */
void write_delayed_enqueue(wchunk *wc) {
	struct timeval tv;
	gettimeofday(&tv,NULL);
	queue_put(dqueue,tv.tv_sec,tv.tv_usec,(uint8_t*)wc,0);
}

/* glock: UNUSED */
void write_enqueue(wchunk *wc) {
	queue_put(jqueue,0,0,(uint8_t*)wc,0);
}

/* worker thread | glock: UNUSED */
void* write_dqueue_worker(void *arg) {
	struct timeval tv;
	uint32_t sec,usec,zero;
	uint8_t *wc;
	(void)arg;
	for (;;) {
		queue_get(dqueue,&sec,&usec,&wc,&zero);
		gettimeofday(&tv,NULL);
		if ((uint32_t)(tv.tv_usec) < usec) {
			tv.tv_sec--;
			tv.tv_usec += 1000000;
		}
		if ((uint32_t)(tv.tv_sec) < sec) {
			// time went backward !!!
			sleep(1);
		} else if ((uint32_t)(tv.tv_sec) == sec) {
			usleep(1000000-(tv.tv_usec-usec));
		}
		queue_put(jqueue,0,0,wc,0);
	}
	return NULL;
}

/* glock: UNLOCKED */
void write_job_end(wchunk *wc,int status,int delay) {
	uint32_t wch;
	wchunk **wcp;
	cblock *cb,*fcb;

	pthread_mutex_lock(&glock);
	if (status) {
		wc->id->status = status;
	}
	status = wc->id->status;

	if (wc->datachainhead && status==0) {
		// reset write id
		for (cb=wc->datachainhead ; cb ; cb=cb->next) {
			cb->writeid = 0;
		}
		if (delay) {
			write_delayed_enqueue(wc);
		} else {
			write_enqueue(wc);
		}
	} else {

		// release left blocks (only when status!=STATUS_OK)
		cb = wc->datachainhead;
		while (cb) {
			fcb = cb;
			cb = cb->next;
			write_cb_release(fcb);
			wc->id->cacheblocks--;
		}
		if (wc->id->cachewaiting>0) {
			pthread_cond_broadcast(&(wc->id->cachecond));
		}

		// remove wchunk entry
		wch = WCHASH(wc->inode,wc->chindx);
		wcp = &(wchash[wch]);
		while ((*wcp) && (*wcp)!=wc) {
			wcp = &((*wcp)->next);
		}
		if (*wcp) {
			*wcp = wc->next;
		} else {
			syslog(LOG_ERR,"internal structure inconsistency error !!!");
			abort();
		}
		close(wc->pipe[0]);
		close(wc->pipe[1]);

		// decrease job counter
		wc->id->jcnt--;
//		syslog(LOG_NOTICE,"job end: jcnt:%"PRIu32",flushwaiting:%"PRIu32,wc->id->jcnt,wc->id->flushwaiting);
		if (wc->id->jcnt==0 && wc->id->flushwaiting>0) {
			pthread_cond_broadcast(&(wc->id->flushcond));
		}
		free(wc);
	}
	pthread_mutex_unlock(&glock);
}

/* main working thread | glock:UNLOCKED */
void* write_worker(void *arg) {
	uint32_t z1,z2,z3;
	uint8_t *data;
	int fd;
	int i;
	struct pollfd pfd[2];
	uint32_t sent,rcvd;
	uint8_t recvbuff[21];
	uint8_t sendbuff[32];
#ifdef HAVE_WRITEV
	struct iovec siov[2];
#endif
	uint8_t pipebuff[1024];
	uint8_t *wptr;
	const uint8_t *rptr;

	uint32_t reccmd;
	uint32_t recleng;
	uint64_t recchunkid;
	uint32_t recwriteid;
	uint8_t recstatus;

	uint32_t partialblocks;

	uint32_t ip;
	uint16_t port;
	uint64_t mfleng;
	uint64_t chunkid;
	uint32_t version;
	uint32_t nextwriteid;
	const uint8_t *chain;
	uint32_t chainsize;
	const uint8_t *csdata;
	uint32_t csdatasize;
	uint8_t westatus;
	uint8_t wrstatus;
	int status;
	uint8_t waitforstatus;
	uint8_t havedata;
	struct timeval start,now;

	uint8_t cnt;

	wchunk *wc;
	cblock *cb,*rcb;
//	inodedata *id;

	(void)arg;
	for (;;) {
		// get next job
		queue_get(jqueue,&z1,&z2,&data,&z3);
		wc = (wchunk*)data;

		pthread_mutex_lock(&glock);
		status = wc->id->status;
		pthread_mutex_unlock(&glock);

		if (status) {
			write_job_end(wc,status,0);
			continue;
		}

		// get chunk data from master
		wrstatus = fs_writechunk(wc->inode,wc->chindx,&mfleng,&chunkid,&version,&csdata,&csdatasize);
		if (wrstatus!=STATUS_OK) {
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - fs_writechunk returns status %d",wc->inode,wc->chindx,chunkid,version,wrstatus);
			if (wrstatus!=ERROR_LOCKED) {
				if (wrstatus==ERROR_ENOENT) {
					write_job_end(wc,EBADF,0);
				} else if (wrstatus==ERROR_QUOTA) {
					write_job_end(wc,EDQUOT,0);
				} else if (wrstatus==ERROR_NOSPACE) {
					write_job_end(wc,ENOSPC,0);
				} else {
					wc->trycnt++;
					if (wc->trycnt>=MAXRETRIES) {
						if (wrstatus==ERROR_NOCHUNKSERVERS) {
							write_job_end(wc,ENOSPC,0);
						} else {
							write_job_end(wc,EIO,0);
						}
					} else {
						write_delayed_enqueue(wc);
					}
				}
			} else {
				write_delayed_enqueue(wc);
			}
			continue;	// get next job
		}
		if (csdata==NULL || csdatasize==0) {
			syslog(LOG_WARNING,"file: %"PRIu32", index: %"PRIu32", chunk: %"PRIu64", version: %"PRIu32" - there are no valid copies",wc->inode,wc->chindx,chunkid,version);
			wc->trycnt++;
			if (wc->trycnt>=MAXRETRIES) {
				write_job_end(wc,ENXIO,0);
			} else {
				write_delayed_enqueue(wc);
			}
			continue;
		}
		chain = csdata;
		ip = get32bit(&chain);
		port = get16bit(&chain);
		chainsize = csdatasize-6;
		gettimeofday(&start,NULL);

		// make connection to cs
		fd = tcpsocket();
		if (fd<0) {
			syslog(LOG_WARNING,"can't create tcp socket: %m");
			fs_writeend(chunkid,wc->inode,mfleng);
			wc->trycnt++;
			if (wc->trycnt>=MAXRETRIES) {
				write_job_end(wc,EIO,0);
			} else {
				write_delayed_enqueue(wc);
			}
			continue;
		}
		if (tcpnodelay(fd)<0) {
			syslog(LOG_WARNING,"can't set TCP_NODELAY: %m");
		}
/*
		if (csdatasize>CSDATARESERVE) {
			csdatasize = CSDATARESERVE;
		}
		memcpy(wrec->csdata,csdata,csdatasize);
		wrec->csdatasize=csdatasize;
		while (csdatasize>=6) {
			tmpip = get32bit(&csdata);
			tmpport = get16bit(&csdata);
			csdatasize-=6;
			csdb_writeinc(tmpip,tmpport);
		}
*/
		if (tcpnumconnect(fd,ip,port)<0) {
			syslog(LOG_WARNING,"can't connect to (%08"PRIX32":%"PRIu16")",ip,port);
			tcpclose(fd);
			fs_writeend(chunkid,wc->inode,mfleng);
			wc->trycnt++;
			if (wc->trycnt>=MAXRETRIES) {
				write_job_end(wc,EIO,0);
			} else {
				write_delayed_enqueue(wc);
			}
			continue;
		}

		partialblocks=0;
		nextwriteid=1;

		pfd[0].fd = fd;
		pfd[1].fd = wc->pipe[0];
		rcvd = 0;
		sent = 0;
		waitforstatus=1;
		havedata=1;
		wptr = sendbuff;
		put32bit(&wptr,CUTOCS_WRITE);
		put32bit(&wptr,12+chainsize);
		put64bit(&wptr,chunkid);
		put32bit(&wptr,version);
// debug:	syslog(LOG_NOTICE,"writeworker: init packet prepared");
		cb = NULL;

		status = 0;
		wrstatus = STATUS_OK;

		do {
			gettimeofday(&now,NULL);
			if (now.tv_usec<start.tv_usec) {
				now.tv_sec--;
				now.tv_usec+=1000000;
			}
			now.tv_sec -= start.tv_sec;
			now.tv_usec -= start.tv_usec;

			if (havedata==0 && now.tv_sec<2) {
				pthread_mutex_lock(&glock);
				if (cb==NULL) {
					if (wc->datachainhead) {
						cb = wc->datachainhead;
						havedata=1;
					}
				} else {
					if (cb->next) {
						cb = cb->next;
						havedata=1;
					} else {
						wc->waitingworker=1;
					}
				}
				if (havedata==1) {
					cb->writeid = nextwriteid++;
// debug:				syslog(LOG_NOTICE,"writeworker: data packet prepared (writeid:%"PRIu32",pos:%"PRIu16")",cb->writeid,cb->pos);
					waitforstatus++;
					wptr = sendbuff;
					put32bit(&wptr,CUTOCS_WRITE_DATA);
					put32bit(&wptr,24+(cb->to-cb->from));
					put64bit(&wptr,chunkid);
					put32bit(&wptr,cb->writeid);
					put16bit(&wptr,cb->pos);
					put16bit(&wptr,cb->from);
					put32bit(&wptr,cb->to-cb->from);
					put32bit(&wptr,mycrc32(0,cb->data+cb->from,cb->to-cb->from));
					if (cb->to-cb->from<65536) {
						partialblocks++;
					}
					sent=0;
				}
				pthread_mutex_unlock(&glock);
			}

			pfd[0].events = POLLIN | (havedata?POLLOUT:0);
			pfd[0].revents = 0;
			pfd[1].events = POLLIN;
			pfd[1].revents = 0;
			if (poll(pfd,2,100)<0) { /* correct timeout - in msec */
				syslog(LOG_WARNING,"writeworker: poll error: %m");
				status=EIO;
				break;
			}
			if (pfd[1].revents&POLLIN) {	// used just to break poll - so just read all data from pipe to empty it
				i = read(wc->pipe[0],pipebuff,1024);
			}
			if (pfd[0].revents&POLLIN) {
				i = read(fd,recvbuff+rcvd,21-rcvd);
				if (i==0) { 	// connection reset by peer
					syslog(LOG_WARNING,"writeworker: connection reset by peer");
					status=EIO;
					break;
				}
				rcvd+=i;
				if (rcvd==21) {
					rptr = recvbuff;
					reccmd = get32bit(&rptr);
					recleng = get32bit(&rptr);
					recchunkid = get64bit(&rptr);
					recwriteid = get32bit(&rptr);
					recstatus = get8bit(&rptr);
					if (reccmd!=CSTOCU_WRITE_STATUS ||  recleng!=13) {
						syslog(LOG_WARNING,"writeworker: got unrecognized packet from chunkserver (cmd:%"PRIu32",leng:%"PRIu32")",reccmd,recleng);
						status=EIO;
						break;
					}
					if (recchunkid!=chunkid) {
						syslog(LOG_WARNING,"writeworker: got unexpected packet (expected chunkdid:%"PRIu64",packet chunkid:%"PRIu64")",chunkid,recchunkid);
						status=EIO;
						break;
					}
					if (recstatus!=STATUS_OK) {
						syslog(LOG_WARNING,"writeworker: write error: %"PRIu8,recstatus);
						wrstatus=recstatus;
						break;
					}
// debug:				syslog(LOG_NOTICE,"writeworker: received status ok for writeid:%"PRIu32,recwriteid);
					if (recwriteid>0) {
						pthread_mutex_lock(&glock);
						for (rcb = wc->datachainhead ; rcb && rcb->writeid!=recwriteid ; rcb=rcb->next) {}
						if (rcb==NULL) {
							syslog(LOG_WARNING,"writeworker: got unexpected status (writeid:%"PRIu32")",recwriteid);
							pthread_mutex_unlock(&glock);
							status=EIO;
							break;
						}
						if (rcb==cb) {	// current block
// debug:						syslog(LOG_NOTICE,"writeworker: received status for current block");
							if (havedata) {	// got status ok before all data had been sent - error
								syslog(LOG_WARNING,"writeworker: got status OK before all data have been sent");
								pthread_mutex_unlock(&glock);
								status=EIO;
								break;
							} else {
								cb = NULL;
							}
						}
						if (rcb->prev) {
							rcb->prev->next = rcb->next;
						} else {
							wc->datachainhead = rcb->next;
						}
						if (rcb->next) {
							rcb->next->prev = rcb->prev;
						} else {
							wc->datachaintail = rcb->prev;
						}
						write_cb_release(rcb);
						wc->id->cacheblocks--;
						if (wc->id->cachewaiting>0) {
							pthread_cond_broadcast(&(wc->id->cachecond));
						}
						pthread_mutex_unlock(&glock);
					}
					waitforstatus--;
					rcvd=0;
				}
			}
			if (havedata && pfd[0].revents&POLLOUT) {
				if (cb==NULL) {	// havedata==1 && cb==NULL means sending first packet (CUTOCS_WRITE)
					if (sent<20) {
#ifdef HAVE_WRITEV
						if (chainsize>0) {
							siov[0].iov_base = sendbuff+sent;
							siov[0].iov_len = 20-sent;
							siov[1].iov_base = (char*)chain;	// discard const (safe - because it's used in writev)
							siov[1].iov_len = chainsize;
							i = writev(fd,siov,2);
						} else {
#endif
							i = write(fd,sendbuff+sent,20-sent);
#ifdef HAVE_WRITEV
						}
#endif
					} else {
						i = write(fd,chain+(sent-20),chainsize-(sent-20));
					}
					if (i<0) {
						syslog(LOG_WARNING,"writeworker: connection reset by peer");
						status=EIO;
						break;
					}
					sent+=i;
					if (sent==20+chainsize) {
						havedata=0;
					}
				} else {
					if (sent<32) {
#ifdef HAVE_WRITEV
						siov[0].iov_base = sendbuff+sent;
						siov[0].iov_len = 32-sent;
						siov[1].iov_base = cb->data+cb->from;
						siov[1].iov_len = cb->to-cb->from;
						i = writev(fd,siov,2);
#else
						i = write(fd,sendbuff+sent,32-sent);
#endif
					} else {
						i = write(fd,cb->data+cb->from+(sent-32),cb->to-cb->from-(sent-32));
					}
					if (i<0) {
						syslog(LOG_WARNING,"writeworker: connection reset by peer");
						status=EIO;
						break;
					}
					sent+=i;
					if (sent==32+cb->to-cb->from) {
						havedata=0;
					}
				}
			}
		} while (waitforstatus>0 && now.tv_sec<3);

		wc->waitingworker=0;

		tcpclose(fd);
//		syslog(LOG_NOTICE,"worker wrote %"PRIu32" blocks (%"PRIu32" partial)",nextwriteid-1,partialblocks);

		pthread_mutex_lock(&glock);
		if (wc->id->maxfleng>mfleng) {
			mfleng=wc->id->maxfleng;
		}
		pthread_mutex_unlock(&glock);

		for (cnt=0 ; cnt<10 ; cnt++) {
			westatus = fs_writeend(chunkid,wc->inode,mfleng);
			if (westatus!=STATUS_OK) {
				usleep(100000+(10000<<cnt));
			} else {
				break;
			}
		}

		if (westatus!=STATUS_OK) {
			write_job_end(wc,ENXIO,0);
		} else if (status!=0 || wrstatus!=STATUS_OK) {
			if (wrstatus!=STATUS_OK) {	// convert MFS status to OS errno
				if (wrstatus==ERROR_NOSPACE) {
					status=ENOSPC;
				} else {
					status=EIO;
				}
			}
			wc->trycnt++;
			if (wc->trycnt>=MAXRETRIES) {
				write_job_end(wc,status,0);
			} else {
				write_job_end(wc,0,1);
			}
		} else {
			read_inode_ops(wc->inode);
			write_job_end(wc,0,0);
		}
	}
}

/* API | glock: INITIALIZED,UNLOCKED */
void write_data_init (uint32_t cachesize) {
	uint32_t cacheblocks = (cachesize/65536);
	uint32_t i;
	if (cacheblocks<10) {
		cacheblocks=10;
	}
	maxinodecacheblocks = cacheblocks/20;
	pthread_mutex_init(&glock,0);

	pthread_cond_init(&fcbcond,0);
	fcbwaiting=0;
	freecblockshead = malloc(sizeof(cblock)*cacheblocks);
	for (i=0 ; i<cacheblocks-1 ; i++) {
		freecblockshead[i].next = freecblockshead+(i+1);
	}
	freecblockshead[cacheblocks-1].next = NULL;

	wchash = malloc(sizeof(wchunk*)*WCHASHSIZE);
	for (i=0 ; i<WCHASHSIZE ; i++) {
		wchash[i]=NULL;
	}

	idhash = malloc(sizeof(inodedata*)*IDHASHSIZE);
	for (i=0 ; i<IDHASHSIZE ; i++) {
		idhash[i]=NULL;
	}

	dqueue = queue_new(0);
	jqueue = queue_new(0);

	pthread_create(&dqueue_worker_th,NULL,write_dqueue_worker,NULL);
//	pthread_create(&info_worker_th,NULL,write_info_worker,NULL);
	for (i=0 ; i<WORKERS ; i++) {
		pthread_create(write_worker_th+i,NULL,write_worker,NULL);
	}
}


/* glock: LOCKED */
int write_cb_expand(cblock *cb,uint32_t from,uint32_t to,const uint8_t *data) {
//	pthread_mutex_lock(&(cb->lock));
	if (cb->writeid>0 || from>cb->to || to<cb->from) {	// can't expand
//		pthread_mutex_unlock(&(cb->lock));
		return -1;
	}
	memcpy(cb->data+from,data,to-from);
	if (from<cb->from) {
		cb->from = from;
	}
	if (to>cb->to) {
		cb->to = to;
	}
//	pthread_mutex_unlock(&(cb->lock));
	return 0;
}

/* glock: LOCKED */
wchunk* write_get_wchunk(inodedata *id,uint16_t chindx) {
	uint32_t wch;
	int pfd[2];
	wchunk *wc;
//	inodedata *id;

	wch = WCHASH(id->inode,chindx);
//	pthread_mutex_lock(&wchashlock);
	for (wc = wchash[wch] ; wc ; wc=wc->next) {
		if (wc->inode==id->inode && wc->chindx==chindx) {
//			pthread_mutex_lock(&(wc->lock));
//			pthread_mutex_unlock(&wchashlock);
			return wc;
		}
	}
	if (pipe(pfd)<0) {
		syslog(LOG_WARNING,"pipe error: %m");
//		pthread_mutex_unlock(&wchashlock);
		return NULL;
	}
	wc = malloc(sizeof(wchunk));
	wc->inode = id->inode;
	wc->chindx = chindx;
	wc->trycnt = 0;
	wc->pipe[0] = pfd[0];
	wc->pipe[1] = pfd[1];
	wc->datachainhead = NULL;
	wc->datachaintail = NULL;
	wc->id = id;
	id->jcnt++;
//	pthread_mutex_init(&(wc->lock),NULL);
	wc->next = wchash[wch];
	wchash[wch]=wc;
//	pthread_mutex_lock(&(wc->lock));
//	pthread_mutex_unlock(&wchashlock);
	write_enqueue(wc);

	// increase job counter
//	id = write_get_inodedata(inode);
//	id->jcnt++;
//	pthread_mutex_unlock(&(id->lock));

	return wc;
}

/* glock: UNLOCKED */
int write_block(inodedata *id,uint16_t chindx,uint16_t pos,uint32_t from,uint32_t to,const uint8_t *data) {
	wchunk *wc;
	cblock *cb;
	uint8_t waited;

	pthread_mutex_lock(&glock);
//	syslog(LOG_NOTICE,"write_block: inode:%"PRIu32" chindx:%"PRIu16" pos:%"PRIu16" from:%"PRIu32"  to:%"PRIu32,id->inode,chindx,pos,from,to);
	wc = write_get_wchunk(id,chindx);	// find or create new one
	if (wc==NULL) {
		return -1;
	}
	for (cb=wc->datachaintail ; cb ; cb=cb->prev) {
		if (cb->pos == pos) {
			if (write_cb_expand(cb,from,to,data)==0) {
//				syslog(LOG_NOTICE,"write_block: expand previous cache block");
//				pthread_mutex_unlock(&(wc->lock));
				pthread_mutex_unlock(&glock);
				return 0;
			} else {
				break;
			}
		}
	}

	waited=0;
	id->cachewaiting++;
	while (id->cacheblocks>=maxinodecacheblocks) {
		waited=1;	// during waiting on cond 'wc' can be changed
		pthread_cond_wait(&(id->cachecond),&glock);
	}
	id->cachewaiting--;
	id->cacheblocks++;
//	while (wc->id->cacheblocks*100>cacheblocks*CACHE_PERC_PER_FILE) {
//		pthread_mutex_wait((&wc->id->cbcond),&glock);
//	}
	cb = write_cb_acquire(&waited);	// during waiting on cond 'wc' can be changed
	if (waited) {
		wc = write_get_wchunk(id,chindx);
		if (wc==NULL) {
			write_cb_release(cb);
			id->cacheblocks--;
			if (id->cachewaiting>0) {
				pthread_cond_signal(&(id->cachecond));
			}
			return -1;
		}
	}
//	syslog(LOG_NOTICE,"write_block: acquired new cache block");
	cb->pos = pos;
	cb->from = from;
	cb->to = to;
	memcpy(cb->data+from,data,to-from);
	cb->prev = wc->datachaintail;
	cb->next = NULL;
	if (wc->datachaintail!=NULL) {
		wc->datachaintail->next = cb;
	} else {
		wc->datachainhead = cb;
	}
	wc->datachaintail = cb;
	if (wc->waitingworker) {
		if (write(wc->pipe[1]," ",1)!=1) {
			syslog(LOG_ERR,"can't write to pipe !!!");
		}
		wc->waitingworker=0;
	}
	pthread_mutex_unlock(&glock);
//	pthread_mutex_unlock(&(wc->lock));
	return 0;
}

/* API | glock: UNLOCKED */
int write_data(void *vid,uint64_t offset,uint32_t size,const uint8_t *data) {
	uint16_t chindx;
	uint16_t pos;
	uint32_t from;
	int status;
	inodedata *id = (inodedata*)vid;
//	struct timeval s,e;

//	gettimeofday(&s,NULL);
	pthread_mutex_lock(&glock);
//	syslog(LOG_NOTICE,"write_data: inode:%"PRIu32" offset:%"PRIu64" size:%"PRIu32,id->inode,offset,size);
//	id = write_get_inodedata(inode);
	status = id->status;
	if (status==0) {
		if (offset+size>id->maxfleng) {	// move fleng
			id->maxfleng = offset+size;
		}
		id->writewaiting++;
		while (id->flushwaiting>0) {
			pthread_cond_wait(&(id->writecond),&glock);
		}
		id->writewaiting--;
	}
	pthread_mutex_unlock(&glock);
	if (status!=0) {
		return status;
	}

	chindx = offset>>26;
	pos = (offset&0x3FFFFFF)>>16;
	from = offset&0xFFFF;
	while (size>0) {
		if (size>0x10000-from) {
			if (write_block(id,chindx,pos,from,0x10000,data)<0) {
				return EIO;
			}
			size -= (0x10000-from);
			data += (0x10000-from);
			from = 0;
			pos++;
			if (pos==1024) {
				pos = 0;
				chindx++;
			}
		} else {
			if (write_block(id,chindx,pos,from,from+size,data)<0) {
				return EIO;
			}
			size = 0;
		}
	}
//	gettimeofday(&e,NULL);
//	syslog(LOG_NOTICE,"write_data time: %"PRId64,TIMEDIFF(e,s));
	return 0;
}

/* API | glock: UNLOCKED */
void* write_data_new(uint32_t inode) {
	inodedata* id;
	pthread_mutex_lock(&glock);
	id = write_get_inodedata(inode);
	id->lcnt++;
//	pthread_mutex_unlock(&(id->lock));
	pthread_mutex_unlock(&glock);
	return id;
}

int write_data_flush(void *vid) {
	inodedata* id = (inodedata*)vid;
	int ret;
//	struct timeval s,e;

//	gettimeofday(&s,NULL);
	pthread_mutex_lock(&glock);
//	id = write_get_inodedata(inode);
	id->flushwaiting++;
	while (id->jcnt>0) {
//		syslog(LOG_NOTICE,"flush: wait ...");
		pthread_cond_wait(&(id->flushcond),&glock);
//		syslog(LOG_NOTICE,"flush: woken up");
	}
	id->flushwaiting--;
	if (id->flushwaiting==0 && id->writewaiting>0) {
		pthread_cond_broadcast(&(id->writecond));
	}
	ret = id->status;
	if (id->lcnt==0 && id->jcnt==0 && id->flushwaiting==0 && id->writewaiting==0) {
		write_free_inodedata(id);
	}
	pthread_mutex_unlock(&glock);
//	gettimeofday(&e,NULL);
//	syslog(LOG_NOTICE,"write_data_flush time: %"PRId64,TIMEDIFF(e,s));
	return ret;
}

uint64_t write_data_getmaxfleng(uint32_t inode) {
	uint64_t maxfleng;
	inodedata* id;
	pthread_mutex_lock(&glock);
	id = write_find_inodedata(inode);
	if (id) {
		maxfleng = id->maxfleng;
	} else {
		maxfleng = 0;
	}
	pthread_mutex_unlock(&glock);
	return maxfleng;
}

/* API | glock: UNLOCKED */
int write_data_flush_inode(uint32_t inode) {
	inodedata* id;
	int ret;
	pthread_mutex_lock(&glock);
	id = write_get_inodedata(inode);
	id->flushwaiting++;
	while (id->jcnt>0) {
//		syslog(LOG_NOTICE,"flush_inode: wait ...");
		pthread_cond_wait(&(id->flushcond),&glock);
//		syslog(LOG_NOTICE,"flush_inode: woken up");
	}
	id->flushwaiting--;
	if (id->flushwaiting==0 && id->writewaiting>0) {
		pthread_cond_broadcast(&(id->writecond));
	}
	ret = id->status;
	if (id->lcnt==0 && id->jcnt==0 && id->flushwaiting==0 && id->writewaiting==0) {
		write_free_inodedata(id);
	}
	pthread_mutex_unlock(&glock);
	return ret;
}

/* API | glock: UNLOCKED */
int write_data_end(void *vid) {
	inodedata* id = (inodedata*)vid;
	int ret;
	pthread_mutex_lock(&glock);
//	id = write_get_inodedata(inode);
	id->flushwaiting++;
	while (id->jcnt>0) {
//		syslog(LOG_NOTICE,"write_end: wait ...");
		pthread_cond_wait(&(id->flushcond),&glock);
//		syslog(LOG_NOTICE,"write_end: woken up");
	}
	id->flushwaiting--;
	if (id->flushwaiting==0 && id->writewaiting>0) {
		pthread_cond_broadcast(&(id->writecond));
	}
	ret = id->status;
	id->lcnt--;
	if (id->lcnt==0 && id->jcnt==0 && id->flushwaiting==0 && id->writewaiting==0) {
		write_free_inodedata(id);
	}
	pthread_mutex_unlock(&glock);
	return ret;
}
