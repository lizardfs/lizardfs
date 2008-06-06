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
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/time.h>
#include <time.h>
#include <syslog.h>
#include <inttypes.h>
#include <pthread.h>

#include "MFSCommunication.h"
#include "sockets.h"
#include "datapack.h"
#include "readdata.h"
#include "mastercomm.h"
#include "cscomm.h"

#define RETRIES 30
#define BUFFERS 4
#define REFRESHTIMEOUT 5000000
#define WRITEDELAY 1000000

#define MAPBITS 10
#define MAPSIZE (1<<(MAPBITS))
#define MAPMASK (MAPSIZE-1)
#define MAPINDX(inode) (inode&MAPMASK)

typedef struct _buffer {
	uint8_t *data;
	uint16_t blockno;
	uint16_t offset;
	uint32_t size;
	uint32_t writeid;
} buffer;

typedef struct _writerec {
	buffer buffers[BUFFERS];
	uint32_t buffid;
	uint32_t nextwriteid;

	uint32_t inode;
	uint64_t fleng;
	uint32_t indx;
	uint64_t chunkid;
	uint32_t version;	// debug purpouses only
	int fd;
	struct timeval vtime;
	struct timeval atime;
	int err;
	int valid;	// # of references
	pthread_mutex_t lock;
	struct _writerec *next;
	struct _writerec *mapnext;
} writerec;

static writerec *wrinodemap[MAPSIZE];
static writerec *wrhead=NULL;
static pthread_t pthid;
static pthread_mutex_t *mainlock;

#define TIMEDIFF(tv1,tv2) (((int64_t)((tv1).tv_sec-(tv2).tv_sec))*1000000LL+(int64_t)((tv1).tv_usec-(tv2).tv_usec))

static void write_data_close_connection(writerec *wrec) {
	if (wrec->fd>=0) {
//		fprintf(stderr,"close connection: %d (chunkid:%llu ; inode:%u ; fleng:%llu)\n",wrec->fd,wrec->chunkid,wrec->inode,wrec->fleng);
		fs_writeend(wrec->chunkid,wrec->inode,wrec->fleng);
		tcpclose(wrec->fd);
		wrec->fd = -1;
		read_inode_ops(wrec->inode);
	}
}

static int write_data_new_connection(writerec *wrec) {
	uint32_t ip;
	uint16_t port;
	uint8_t *chain;
	uint32_t chainsize;
//	uint32_t version;
	int status;
	if (wrec->fd>=0) {
		write_data_close_connection(wrec);
	}
	do {
		status = fs_writechunk(wrec->inode,wrec->indx,&(wrec->fleng),&(wrec->chunkid),&(wrec->version),&ip,&port,&chain,&chainsize);
//		fprintf(stderr,"writechunk status: %d\n",status);
		if (status!=0 && status!=ERROR_LOCKED) {
			syslog(LOG_WARNING,"file: %u, index: %u, chunk: %llu, version: %u - fs_writechunk returns status %u",wrec->inode,wrec->indx,(unsigned long long int)(wrec->chunkid),wrec->version,status);
			if (status==ERROR_ENOENT) {
				return -2;
			}
			return -1;
		}
		if (status==ERROR_LOCKED) {
			sleep(1);
		}
	} while (status==ERROR_LOCKED);
	if (ip==0 || port==0) {
		syslog(LOG_WARNING,"file: %u, index: %u, chunk: %llu, version: %u - there are no valid copies",wrec->inode,wrec->indx,(unsigned long long int)(wrec->chunkid),wrec->version);
		return -3;
	}
	gettimeofday(&(wrec->vtime),NULL);
	wrec->fd = tcpsocket();
	if (wrec->fd<0) {
		syslog(LOG_WARNING,"can't create tcp socket: %m");
		fs_writeend(wrec->chunkid,wrec->inode,wrec->fleng);
		return -1;
	}
	if (tcpnodelay(wrec->fd)<0) {
		syslog(LOG_WARNING,"can't set TCP_NODELAY: %m");
	}
	if (tcpnumconnect(wrec->fd,ip,port)<0) {
		syslog(LOG_WARNING,"can't connect to (%08X:%u)",ip,port);
		write_data_close_connection(wrec);
		return -1;
	}
	if (cs_writeinit(wrec->fd,chain,chainsize,wrec->chunkid,wrec->version)<0) {
		syslog(LOG_WARNING,"file: %u, index: %u, chunk: %llu, version: %u - writeinit error",wrec->inode,wrec->indx,(unsigned long long int)(wrec->chunkid),wrec->version);
		write_data_close_connection(wrec);
		return -1;
	}
	wrec->nextwriteid=1;
//	fprintf(stderr,"new connection: %d (chunkid:%llu ; inode:%u ; fleng:%llu)\n",wrec->fd,wrec->chunkid,wrec->inode,wrec->fleng);
	return 0;
}



static int write_data_buffer_send(writerec *wrec,uint32_t buffid) {
	int i;
	buffer *b;
	b = wrec->buffers + buffid;
	if (b->size>0) {
//		fprintf(stderr,"send buffid: %u\n",buffid);
		b->writeid = wrec->nextwriteid;
		wrec->nextwriteid++;
//		fprintf(stderr,"send %d filled from %d to %d (id:%d)\n",b->blockno,b->offset,b->offset+b->size-1,b->writeid);
		i = cs_writeblock(wrec->fd,wrec->chunkid,b->writeid,b->blockno,b->offset,b->size,b->data+b->offset);
		if (i<0) {
			syslog(LOG_WARNING,"file: %u, index: %u, chunk: %llu, version: %u - writeblock error",wrec->inode,wrec->indx,(unsigned long long int)(wrec->chunkid),wrec->version);
		}
		return i;
	}
	return 0;
}

static int write_data_buffer_check(writerec *wrec,uint32_t buffid) {
	int i;
	buffer *b;
	b = wrec->buffers + buffid;
//	fprintf(stderr,"check buff id: %u, (id:%d)\n",buffid,b->writeid);
	if (b->writeid>0) {
//		fprintf(stderr,"check %d (id:%d)\n",b->blockno,b->writeid);
		i = cs_writestatus(wrec->fd,wrec->chunkid,b->writeid);
//		fprintf(stderr,"write status: %d\n",i);
		if (i==0) {
			uint64_t nleng;
			nleng = wrec->indx;
			nleng<<=26;
			nleng+=((uint32_t)(b->blockno))<<16;
			nleng+=(b->offset+b->size);
			//fprintf(stderr,"(%u,%u,%u -> %llu)\n",wrec->indx,wrec->blockno,wrec->blockoffset+wrec->blocksize,nleng);
			if (nleng>wrec->fleng) {
				wrec->fleng = nleng;
			}
			b->writeid = 0;
			b->size = 0;
			b->offset = 0;
		} else {
			syslog(LOG_WARNING,"file: %u, index: %u, chunk: %llu, version: %u - writestatus error",wrec->inode,wrec->indx,(unsigned long long int)(wrec->chunkid),wrec->version);
		}
		return i;
	}
	return 0;
}

static int write_data_error_reconnect(writerec *wrec) {
	int err;
	uint32_t bid;
	write_data_close_connection(wrec);
	for (bid = 0 ; bid<BUFFERS ; bid++) {
		wrec->buffers[bid].writeid = 0;
	}
	err = write_data_new_connection(wrec);
	if (err<0) {
		return err;
	}
	for (bid = 0 ; bid<BUFFERS ; bid++) {
		err = write_data_buffer_send(wrec,(bid+wrec->buffid+1)%BUFFERS);
		if (err<0) {
			return err;
		}
	}
	return 0;
}

static int write_data_wait_for_all(writerec *wrec) {
	uint32_t bid;
	int err;
	for (bid = 0 ; bid<BUFFERS ; bid++) {
		err = write_data_buffer_check(wrec,(bid+wrec->buffid+1)%BUFFERS);
		if (err<0) {
			return err;
		}
	}
	return 0;
}

/*
static int write_data_send_all(writerec *wrec) {
	uint32_t bid;
	for (bid =0 ; bid<BUFFERS ; bid++) {
		if (write_data_buffer_send(wrec,(bid+wrec->buffid+1)%BUFFERS)<0) {
			return -1;
		}
	}
	return 0;
}
*/

static void write_data_do_flush(writerec *wrec) {
	int err;
	uint32_t cnt;
	if (wrec->err<0) {
		return;
	}
	err=0;
	if (wrec->buffers[wrec->buffid].size>0) {
		if (wrec->fd<0) {
			err = write_data_new_connection(wrec);
		}
		if (err==0) {
			err = write_data_buffer_send(wrec,wrec->buffid);
		}
	}
	if (err==-2) {	// no such inode - unrecoverable error
		wrec->err = err;
		return;
	}
	for (cnt=0 ; cnt<RETRIES ; cnt++) {
		if (err==0) {
			err = write_data_wait_for_all(wrec);
			if (err==0) {
				write_data_close_connection(wrec);
				return;
			}
		}
		err = write_data_error_reconnect(wrec);
		if (err<0) {
			syslog(LOG_WARNING,"file: %u, index: %u, chunk: %llu, version: %u - write(flush) error (try counter: %u)",wrec->inode,wrec->indx,(unsigned long long int)(wrec->chunkid),wrec->version,cnt);
			if (err==-2) {	// no such inode - unrecoverable error
				wrec->err = err;
				return;
			}
			if (err==-3) {	// chunk not available - wait longer
				sleep(60);
				cnt+=9;
			} else {
				sleep(1+cnt/5);
			}
		}
	}
	if (err==0) {
		err = write_data_wait_for_all(wrec);
		if (err==0) {
			write_data_close_connection(wrec);
			return;
		}
	}
	wrec->err = err;
	return;
}

static void write_data_next_buffer(writerec *wrec) {
	uint32_t cnt;
	uint32_t bid;
	int err;
	bid = wrec->buffid;
	if (wrec->fd<0) {
		err = write_data_new_connection(wrec);
	} else {
		err = 0;
	}
	if (err==-2) {	// no such inode - unrecoverable error
		wrec->err = err;
		return;
	}
	if (err==0) {
		err = write_data_buffer_send(wrec,bid);
		if (err==0) {
			bid = (bid+1)%BUFFERS;
			err = write_data_buffer_check(wrec,bid);
			if (err==0) {
				wrec->buffid = bid;
				return;
			}
		}
	}
	bid = wrec->buffid;
	for (cnt=0 ; cnt<RETRIES ; cnt++) {
		err = write_data_error_reconnect(wrec);
		if (err<0) {
			syslog(LOG_WARNING,"file: %u, index: %u, chunk: %llu, version: %u - writedata error (try counter: %u)",wrec->inode,wrec->indx,(unsigned long long int)(wrec->chunkid),wrec->version,cnt);
			if (err==-2) {	// no such inode - unrecoverable error
				wrec->err = err;
				return;
			}
			if (err==-3) {	// chunk not available - wait longer
				sleep(60);
				cnt+=9;
			} else {
				sleep(1+cnt/5);
			}
		} else {
			bid = (bid+1)%BUFFERS;
			err = write_data_buffer_check(wrec,bid);
			if (err==0) {
				wrec->buffid = bid;
				return;
			}
		}
	}
	wrec->err = err;
	return ;
}

/*
static void write_data_do_flush(writerec *wrec) {
	uint8_t cnt;
	struct timeval now;
	if (wrec->err<0) {
		return;
	}
	cnt = 0;
	while (wrec->blocksize>0) {
		gettimeofday(&now,NULL);
		if (wrec->fd<0 || TIMEDIFF(now,wrec->vtime)>REFRESHTIMEOUT) {
			while (cnt<RETRIES) {
				cnt++;
				if (write_data_refresh_connection(wrec)==0) {
					break;
				}
				sleep(1);
			}
			if (cnt==RETRIES) {
				wrec->err = -1;
				return;
			}
			gettimeofday(&(wrec->vtime),NULL);
		}
//		fprintf(stderr,"(%p,%u,%llu,%u,%u,%u,%u,%p)\n",wrec->chain,wrec->chainsize,wrec->chunkid,wrec->version,wrec->blockno,wrec->blockoffset,wrec->blocksize,wrec->wbuff+wrec->blockoffset);
		if (cs_writeblock(wrec->fd,wrec->chunkid,wrec->writeid,wrec->blockno,wrec->blockoffset,wrec->blocksize,wrec->wbuff+wrec->blockoffset)<0) {
			wrec->vtime.tv_sec = wrec->vtime.tv_usec = 0;
			sleep(1);
		} else if (cs_writestatus(wrec->fd,wrec->chunkid,wrec->writeid)<0) {
			wrec->vtime.tv_sec = wrec->vtime.tv_usec = 0;
			sleep(1);
		} else {
			uint64_t nleng;
			nleng = wrec->indx;
			nleng<<=26;
			nleng+=((uint32_t)(wrec->blockno))<<16;
			nleng+=(wrec->blockoffset+wrec->blocksize);
//			fprintf(stderr,"(%u,%u,%u -> %llu)\n",wrec->indx,wrec->blockno,wrec->blockoffset+wrec->blocksize,nleng);
			if (nleng>wrec->fleng) {
				wrec->fleng = nleng;
			}
			wrec->blockno = 0;
			wrec->blockoffset = 0;
			wrec->blocksize = 0;
		}
	}
}
*/

void* write_data_delayed_ops(void *arg) {
	struct timeval now;
	writerec *wrec,**wrecp;
	writerec **wrecmap;
	uint32_t bid;
	(void)arg;
	for (;;) {
		pthread_mutex_lock(mainlock);
		wrecp = &wrhead;
		while ((wrec=*wrecp)!=NULL) {
			if (wrec->valid==0) {
				for (bid=0 ; bid<BUFFERS ; bid++) {
					free(wrec->buffers[bid].data);
				}
				pthread_mutex_lock(&(wrec->lock));
				pthread_mutex_unlock(&(wrec->lock));
				pthread_mutex_destroy(&(wrec->lock));
				*wrecp = wrec->next;
				wrecmap = &(wrinodemap[MAPINDX(wrec->inode)]);
				while (*wrecmap) {
					if ((*wrecmap)==wrec) {
						*wrecmap = wrec->mapnext;
					} else {
						wrecmap = &((*wrecmap)->mapnext);
					}
				}
				free(wrec);
			} else {
				pthread_mutex_lock(&(wrec->lock));
				gettimeofday(&now,NULL);
				if (wrec->err==0 && (wrec->fd>=0 || wrec->buffers[wrec->buffid].size>0) && (TIMEDIFF(now,wrec->atime)>WRITEDELAY || TIMEDIFF(now,wrec->vtime)>REFRESHTIMEOUT)) {
//					fprintf(stderr,"delayed write flush (d1: %llu ; d2: %llu)\n",TIMEDIFF(now,wrec->atime),TIMEDIFF(now,wrec->vtime));
					write_data_do_flush(wrec);
				}
				pthread_mutex_unlock(&(wrec->lock));
				wrecp = &(wrec->next);
			}
		}
		pthread_mutex_unlock(mainlock);
		usleep(WRITEDELAY/2);
	}
}

void write_data_init() {
	uint32_t i;
	for (i=0 ; i<MAPSIZE ; i++) {
		wrinodemap[i]=NULL;
	}
	mainlock = malloc(sizeof(pthread_mutex_t));
	pthread_mutex_init(mainlock,NULL);
	pthread_create(&pthid,NULL,write_data_delayed_ops,NULL);
}

void* write_data_new(uint32_t inode) {
	writerec *wrec;
	buffer *b;
	uint32_t bid;
	pthread_mutex_lock(mainlock);
	for (wrec=wrinodemap[MAPINDX(inode)] ; wrec ; wrec=wrec->mapnext) {
		if (wrec->inode == inode) {
			wrec->valid++;
			pthread_mutex_unlock(mainlock);
			return wrec;
		}
	}
	wrec = malloc(sizeof(writerec));
	for (bid=0 ; bid<BUFFERS ; bid++) {
		b = wrec->buffers + bid;
		b->data = malloc(65536);
		b->blockno = 0;
		b->offset = 0;
		b->size = 0;
		b->writeid = 0;
	}
	wrec->buffid = 0;
	wrec->nextwriteid = 0;
	wrec->inode = inode;
	wrec->fleng = 0;
	wrec->indx = 0;
	wrec->chunkid = 0;
	wrec->fd = -1;
	pthread_mutex_init(&(wrec->lock),NULL);
	gettimeofday(&(wrec->vtime),NULL);
	wrec->atime.tv_sec = 0;
	wrec->atime.tv_usec = 0;
	wrec->err = 0;
	wrec->valid = 1;
	wrec->next = wrhead;
	wrhead = wrec;
	wrec->mapnext = wrinodemap[MAPINDX(inode)];
	wrinodemap[MAPINDX(inode)] = wrec;
	pthread_mutex_unlock(mainlock);
	return wrec;
}

void write_data_flush_inode(uint32_t inode) {
	writerec *wrec;
	pthread_mutex_lock(mainlock);
	for (wrec = wrinodemap[MAPINDX(inode)]; wrec ; wrec=wrec->mapnext) {
		if (wrec->inode==inode) {
			pthread_mutex_lock(&(wrec->lock));
			if (wrec->valid>0 && wrec->err==0) {
				write_data_do_flush(wrec);
			}
			pthread_mutex_unlock(&(wrec->lock));
		}
	}
	pthread_mutex_unlock(mainlock);
}

int write_data_flush(void *wr) {
	writerec *wrec = (writerec*)wr;
	pthread_mutex_lock(&(wrec->lock));
	write_data_do_flush(wrec);
	pthread_mutex_unlock(&(wrec->lock));
	return wrec->err;
}

void write_data_end(void *wr) {
	writerec *wrec = (writerec*)wr;
	pthread_mutex_lock(&(wrec->lock));
	write_data_do_flush(wrec);
	pthread_mutex_unlock(&(wrec->lock));
	wrec->valid--;
	return;
}


int write_data(void *wr,uint64_t offset,uint32_t size,const uint8_t *buff) {
	writerec *wrec = (writerec*)wr;
	buffer *b;
	uint32_t indx;
	uint16_t cblockno;
	uint32_t cblocksize;
	uint16_t cblockoffset;
	(void)offset;
	(void)size;
	(void)buff;
/* - mo¿e warto potestowaæ z tym postgresql'a ?
	{
		writerec *wp;
		pthread_mutex_lock(mainlock);
		for (wp=wrhead ; wp ; wp=wp->next) {
			if (wrec!=wp) {
				write_data_flush(wp);
			}
		}
		pthread_mutex_unlock(mainlock);
	}
*/
	pthread_mutex_lock(&(wrec->lock));
	while (size>0 && wrec->err==0) {
		indx = (offset>>26);
		cblockno = (offset&0x3FFFFFF)>>16;
		cblockoffset = offset&0xFFFF;
		if (cblockoffset+size>65536) {
			cblocksize = 65536-cblockoffset;
		} else {
			cblocksize = size;
		}
		if (indx!=wrec->indx) {
			write_data_do_flush(wrec);
			wrec->indx = indx;
		}
		if (wrec->err<0) {
			break;
		}
		b = wrec->buffers + wrec->buffid;
		if (b->size>0 && (cblockno!=b->blockno || cblockoffset+cblocksize<b->offset || b->offset+b->size<cblockoffset)) {
			write_data_next_buffer(wrec);
			b = wrec->buffers + wrec->buffid;
		}
		if (wrec->err<0) {
			break;
		}
		if (b->size==0) {
			b->blockno = cblockno;
			b->offset = cblockoffset;
			b->size = cblocksize;
		} else {
			if ((cblockoffset+cblocksize) > (b->offset+b->size)) {
				if (cblockoffset<b->offset) {
					b->offset = cblockoffset;
				}
				b->size = (cblockoffset+cblocksize)-b->offset;
			} else {
				if (cblockoffset<b->offset) {
					b->size += b->offset - cblockoffset;
					b->offset = cblockoffset;
				}
			}
		}
//		fprintf(stderr,"fill buffer %d from %d to %d\n",wrec->buffid,cblockoffset,cblockoffset+cblocksize-1);
//		fprintf(stderr,"buffer %d from %d to %d\n",wrec->buffid,b->offset,b->offset+b->size-1);
		memcpy(b->data+cblockoffset,buff,cblocksize);
		offset+=cblocksize;
		size-=cblocksize;
		buff+=cblocksize;
	}
	gettimeofday(&(wrec->atime),NULL);
	pthread_mutex_unlock(&(wrec->lock));
	return wrec->err;
}
