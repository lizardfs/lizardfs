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

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <syslog.h>
#include <inttypes.h>
//#include <fcntl.h>
//#include <sys/ioctl.h>
#include <limits.h>
#include <pthread.h>
#include <errno.h>

#include "pcqueue.h"
#include "datapack.h"
#include "massert.h"

#include "hddspacemgr.h"
#include "replicator.h"

#define JHASHSIZE 0x400
#define JHASHPOS(id) ((id)&0x3FF)

enum {
	JSTATE_DISABLED,
	JSTATE_ENABLED,
	JSTATE_INPROGRESS
};

enum {
	OP_EXIT,
	OP_INVAL,
	OP_CHUNKOP,
	OP_OPEN,
	OP_CLOSE,
	OP_READ,
	OP_WRITE,
	OP_REPLICATE
};

// for OP_CHUNKOP
typedef struct _chunk_op_args {
	uint64_t chunkid,copychunkid;
	uint32_t version,newversion,copyversion;
	uint32_t length;
} chunk_op_args;

// for OP_OPEN and OP_CLOSE
typedef struct _chunk_oc_args {
	uint64_t chunkid;
} chunk_oc_args;

// for OP_READ
typedef struct _chunk_rd_args {
	uint64_t chunkid;
	uint32_t version;
	uint32_t offset,size;
	uint16_t blocknum;
	uint8_t *buffer;
	uint8_t *crcbuff;
} chunk_rd_args;

// for OP_WRITE
typedef struct _chunk_wr_args {
	uint64_t chunkid;
	uint32_t version;
	uint32_t offset,size;
	uint16_t blocknum;
	const uint8_t *buffer;
	const uint8_t *crcbuff;
} chunk_wr_args;

typedef struct _chunk_rp_args {
	uint64_t chunkid;
	uint32_t version;
	uint8_t srccnt;
} chunk_rp_args;

typedef struct _job {
	uint32_t jobid;
	void (*callback)(uint8_t status,void *extra);
	void *extra;
	void *args;
	uint8_t jstate;
	struct _job *next;
} job;

typedef struct _jobpool {
	int rpipe,wpipe;
	uint8_t workers;
	pthread_t *workerthreads;
	pthread_mutex_t pipelock;
	pthread_mutex_t jobslock;
	void *jobqueue;
	void *statusqueue;
	job* jobhash[JHASHSIZE];
	uint32_t nextjobid;
} jobpool;

static inline void job_send_status(jobpool *jp,uint32_t jobid,uint8_t status) {
	zassert(pthread_mutex_lock(&(jp->pipelock)));
	if (queue_isempty(jp->statusqueue)) {	// first status
		eassert(write(jp->wpipe,&status,1)==1);	// write anything to wake up select
	}
	queue_put(jp->statusqueue,jobid,status,NULL,1);
	zassert(pthread_mutex_unlock(&(jp->pipelock)));
	return;
}

static inline int job_receive_status(jobpool *jp,uint32_t *jobid,uint8_t *status) {
	uint32_t qstatus;
	zassert(pthread_mutex_lock(&(jp->pipelock)));
	queue_get(jp->statusqueue,jobid,&qstatus,NULL,NULL);
	*status = qstatus;
	if (queue_isempty(jp->statusqueue)) {
		eassert(read(jp->rpipe,&qstatus,1)==1);	// make pipe empty
		zassert(pthread_mutex_unlock(&(jp->pipelock)));
		return 0;	// last element
	}
	zassert(pthread_mutex_unlock(&(jp->pipelock)));
	return 1;	// not last
}

#define opargs ((chunk_op_args*)(jptr->args))
#define ocargs ((chunk_oc_args*)(jptr->args))
#define rdargs ((chunk_rd_args*)(jptr->args))
#define wrargs ((chunk_wr_args*)(jptr->args))
#define rpargs ((chunk_rp_args*)(jptr->args))
void* job_worker(void *th_arg) {
	jobpool *jp = (jobpool*)th_arg;
	job *jptr;
	uint8_t *jptrarg;
	uint8_t status,jstate;
	uint32_t jobid;
	uint32_t op;

//	syslog(LOG_NOTICE,"worker %p started (jobqueue: %p ; jptr:%p ; jptrarg:%p ; status:%p )",(void*)pthread_self(),jp->jobqueue,(void*)&jptr,(void*)&jptrarg,(void*)&status);
	for (;;) {
		queue_get(jp->jobqueue,&jobid,&op,&jptrarg,NULL);
		jptr = (job*)jptrarg;
		zassert(pthread_mutex_lock(&(jp->jobslock)));
		if (jptr!=NULL) {
			jstate=jptr->jstate;
			if (jptr->jstate==JSTATE_ENABLED) {
				jptr->jstate=JSTATE_INPROGRESS;
			}
		} else {
			jstate=JSTATE_DISABLED;
		}
		zassert(pthread_mutex_unlock(&(jp->jobslock)));
		switch (op) {
			case OP_INVAL:
				status = ERROR_EINVAL;
				break;
			case OP_CHUNKOP:
				if (jstate==JSTATE_DISABLED) {
					status = ERROR_NOTDONE;
				} else {
					status = hdd_chunkop(opargs->chunkid,opargs->version,opargs->newversion,opargs->copychunkid,opargs->copyversion,opargs->length);
				}
				break;
			case OP_OPEN:
				if (jstate==JSTATE_DISABLED) {
					status = ERROR_NOTDONE;
				} else {
					status = hdd_open(ocargs->chunkid);
				}
				break;
			case OP_CLOSE:
				if (jstate==JSTATE_DISABLED) {
					status = ERROR_NOTDONE;
				} else {
					status = hdd_close(ocargs->chunkid);
				}
				break;
			case OP_READ:
				if (jstate==JSTATE_DISABLED) {
					status = ERROR_NOTDONE;
				} else {
					status = hdd_read(rdargs->chunkid,rdargs->version,rdargs->blocknum,rdargs->buffer,rdargs->offset,rdargs->size,rdargs->crcbuff);
				}
				break;
			case OP_WRITE:
				if (jstate==JSTATE_DISABLED) {
					status = ERROR_NOTDONE;
				} else {
					status = hdd_write(wrargs->chunkid,wrargs->version,wrargs->blocknum,wrargs->buffer,wrargs->offset,wrargs->size,wrargs->crcbuff);
				}
				break;
			case OP_REPLICATE:
				if (jstate==JSTATE_DISABLED) {
					status = ERROR_NOTDONE;
				} else {
					status = replicate(rpargs->chunkid,rpargs->version,rpargs->srccnt,((uint8_t*)(jptr->args))+sizeof(chunk_rp_args));
				}
				break;
			default: // OP_EXIT
//				syslog(LOG_NOTICE,"worker %p exiting (jobqueue: %p)",(void*)pthread_self(),jp->jobqueue);
				return NULL;
		}
		job_send_status(jp,jobid,status);
	}
}

static inline uint32_t job_new(jobpool *jp,uint32_t op,void *args,void (*callback)(uint8_t status,void *extra),void *extra) {
//	jobpool* jp = (jobpool*)jpool;
	uint32_t jobid = jp->nextjobid;
	uint32_t jhpos = JHASHPOS(jobid);
	job *jptr;
	jptr = malloc(sizeof(job));
	passert(jptr);
	jptr->jobid = jobid;
	jptr->callback = callback;
	jptr->extra = extra;
	jptr->args = args;
	jptr->jstate = JSTATE_ENABLED;
	jptr->next = jp->jobhash[jhpos];
	jp->jobhash[jhpos] = jptr;
	queue_put(jp->jobqueue,jobid,op,(uint8_t*)jptr,1);
	jp->nextjobid++;
	if (jp->nextjobid==0) {
		jp->nextjobid=1;
	}
	return jobid;
}

/* interface */

void* job_pool_new(uint8_t workers,uint32_t jobs,int *wakeupdesc) {
	int fd[2];
	uint32_t i;
	pthread_attr_t thattr;
	jobpool* jp;

	if (pipe(fd)<0) {
		return NULL;
	}
       	jp=malloc(sizeof(jobpool));
	passert(jp);
//	syslog(LOG_WARNING,"new pool of workers (%p:%"PRIu8")",(void*)jp,workers);
	*wakeupdesc = fd[0];
	jp->rpipe = fd[0];
	jp->wpipe = fd[1];
	jp->workers = workers;
	jp->workerthreads = malloc(sizeof(pthread_t)*workers);
	passert(jp->workerthreads);
	zassert(pthread_mutex_init(&(jp->pipelock),NULL));
	zassert(pthread_mutex_init(&(jp->jobslock),NULL));
	jp->jobqueue = queue_new(jobs);
//	syslog(LOG_WARNING,"new jobqueue: %p",jp->jobqueue);
	jp->statusqueue = queue_new(0);
	for (i=0 ; i<JHASHSIZE ; i++) {
		jp->jobhash[i]=NULL;
	}
	jp->nextjobid = 1;
	zassert(pthread_attr_init(&thattr));
	zassert(pthread_attr_setstacksize(&thattr,0x100000));
	zassert(pthread_attr_setdetachstate(&thattr,PTHREAD_CREATE_JOINABLE));
	for (i=0 ; i<workers ; i++) {
		zassert(pthread_create(jp->workerthreads+i,&thattr,job_worker,jp));
	}
	zassert(pthread_attr_destroy(&thattr));
	return jp;
}

uint32_t job_pool_jobs_count(void *jpool) {
	jobpool* jp = (jobpool*)jpool;
	return queue_elements(jp->jobqueue);
}

void job_pool_disable_and_change_callback_all(void *jpool,void (*callback)(uint8_t status,void *extra)) {
	jobpool* jp = (jobpool*)jpool;
	uint32_t jhpos;
	job *jptr;

	zassert(pthread_mutex_lock(&(jp->jobslock)));
	for (jhpos = 0 ; jhpos<JHASHSIZE ; jhpos++) {
		for (jptr = jp->jobhash[jhpos] ; jptr ; jptr=jptr->next) {
			if (jptr->jstate==JSTATE_ENABLED) {
				jptr->jstate=JSTATE_DISABLED;
			}
			jptr->callback=callback;
		}
	}
	zassert(pthread_mutex_unlock(&(jp->jobslock)));
}

void job_pool_disable_job(void *jpool,uint32_t jobid) {
	jobpool* jp = (jobpool*)jpool;
	uint32_t jhpos = JHASHPOS(jobid);
	job *jptr;
	for (jptr = jp->jobhash[jhpos] ; jptr ; jptr=jptr->next) {
		if (jptr->jobid==jobid) {
			zassert(pthread_mutex_lock(&(jp->jobslock)));
			if (jptr->jstate==JSTATE_ENABLED) {
				jptr->jstate=JSTATE_DISABLED;
			}
			zassert(pthread_mutex_unlock(&(jp->jobslock)));
		}
	}
}

void job_pool_change_callback(void *jpool,uint32_t jobid,void (*callback)(uint8_t status,void *extra),void *extra) {
	jobpool* jp = (jobpool*)jpool;
	uint32_t jhpos = JHASHPOS(jobid);
	job *jptr;
	for (jptr = jp->jobhash[jhpos] ; jptr ; jptr=jptr->next) {
		if (jptr->jobid==jobid) {
			jptr->callback=callback;
			jptr->extra=extra;
		}
	}
}

void job_pool_check_jobs(void *jpool) {
	jobpool* jp = (jobpool*)jpool;
	uint32_t jobid,jhpos;
	uint8_t status;
	int notlast;
	job **jhandle,*jptr;
	do {
		notlast = job_receive_status(jp,&jobid,&status);
		jhpos = JHASHPOS(jobid);
		jhandle = jp->jobhash+jhpos;
		while ((jptr = *jhandle)) {
			if (jptr->jobid==jobid) {
				if (jptr->callback) {
					jptr->callback(status,jptr->extra);
				}
				*jhandle = jptr->next;
				if (jptr->args) {
					free(jptr->args);
				}
				free(jptr);
				break;
			} else {
				jhandle = &(jptr->next);
			}
		}
	} while (notlast);
}

void job_pool_delete(void *jpool) {
	jobpool* jp = (jobpool*)jpool;
	uint32_t i;
//	syslog(LOG_WARNING,"deleting pool of workers (%p:%"PRIu8")",(void*)jp,jp->workers);
	for (i=0 ; i<jp->workers ; i++) {
		queue_put(jp->jobqueue,0,OP_EXIT,NULL,1);
	}
	for (i=0 ; i<jp->workers ; i++) {
		zassert(pthread_join(jp->workerthreads[i],NULL));
	}
	sassert(queue_isempty(jp->jobqueue));
	if (!queue_isempty(jp->statusqueue)) {
		job_pool_check_jobs(jp);
	}
//	syslog(LOG_NOTICE,"deleting jobqueue: %p",jp->jobqueue);
	queue_delete(jp->jobqueue);
	queue_delete(jp->statusqueue);
	zassert(pthread_mutex_destroy(&(jp->pipelock)));
	zassert(pthread_mutex_destroy(&(jp->jobslock)));
	free(jp->workerthreads);
	close(jp->rpipe);
	close(jp->wpipe);
	free(jp);
}

uint32_t job_inval(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra) {
	jobpool* jp = (jobpool*)jpool;
	return job_new(jp,OP_INVAL,NULL,callback,extra);
}

uint32_t job_chunkop(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint32_t newversion,uint64_t copychunkid,uint32_t copyversion,uint32_t length) {
	jobpool* jp = (jobpool*)jpool;
	chunk_op_args *args;
	args = malloc(sizeof(chunk_op_args));
	passert(args);
	args->chunkid = chunkid;
	args->version = version;
	args->newversion = newversion;
	args->copychunkid = copychunkid;
	args->copyversion = copyversion;
	args->length = length;
	return job_new(jp,OP_CHUNKOP,args,callback,extra);
}

uint32_t job_open(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid) {
	jobpool* jp = (jobpool*)jpool;
	chunk_oc_args *args;
	args = malloc(sizeof(chunk_oc_args));
	passert(args);
	args->chunkid = chunkid;
	return job_new(jp,OP_OPEN,args,callback,extra);
}

uint32_t job_close(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid) {
	jobpool* jp = (jobpool*)jpool;
	chunk_oc_args *args;
	args = malloc(sizeof(chunk_oc_args));
	passert(args);
	args->chunkid = chunkid;
	return job_new(jp,OP_CLOSE,args,callback,extra);
}

uint32_t job_read(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint16_t blocknum,uint8_t *buffer,uint32_t offset,uint32_t size,uint8_t *crcbuff) {
	jobpool* jp = (jobpool*)jpool;
	chunk_rd_args *args;
	args = malloc(sizeof(chunk_rd_args));
	passert(args);
	args->chunkid = chunkid;
	args->version = version;
	args->blocknum = blocknum;
	args->buffer = buffer;
	args->offset = offset;
	args->size = size;
	args->crcbuff = crcbuff;
	return job_new(jp,OP_READ,args,callback,extra);
}

uint32_t job_write(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint16_t blocknum,const uint8_t *buffer,uint32_t offset,uint32_t size,const uint8_t *crcbuff) {
	jobpool* jp = (jobpool*)jpool;
	chunk_wr_args *args;
	args = malloc(sizeof(chunk_wr_args));
	passert(args);
	args->chunkid = chunkid;
	args->version = version;
	args->blocknum = blocknum;
	args->buffer = buffer;
	args->offset = offset;
	args->size = size;
	args->crcbuff = crcbuff;
	return job_new(jp,OP_WRITE,args,callback,extra);
}

uint32_t job_replicate(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint8_t srccnt,const uint8_t *srcs) {
	jobpool* jp = (jobpool*)jpool;
	chunk_rp_args *args;
	uint8_t *ptr;
	ptr = malloc(sizeof(chunk_rp_args)+srccnt*18);
	passert(ptr);
	args = (chunk_rp_args*)ptr;
	ptr += sizeof(chunk_rp_args);
	args->chunkid = chunkid;
	args->version = version;
	args->srccnt = srccnt;
	memcpy(ptr,srcs,srccnt*18);
	return job_new(jp,OP_REPLICATE,args,callback,extra);
}

uint32_t job_replicate_simple(void *jpool,void (*callback)(uint8_t status,void *extra),void *extra,uint64_t chunkid,uint32_t version,uint32_t ip,uint16_t port) {
	jobpool* jp = (jobpool*)jpool;
	chunk_rp_args *args;
	uint8_t *ptr;
	ptr = malloc(sizeof(chunk_rp_args)+18);
	passert(ptr);
	args = (chunk_rp_args*)ptr;
	ptr += sizeof(chunk_rp_args);
	args->chunkid = chunkid;
	args->version = version;
	args->srccnt = 1;
	put64bit(&ptr,chunkid);
	put32bit(&ptr,version);
	put32bit(&ptr,ip);
	put16bit(&ptr,port);
	return job_new(jp,OP_REPLICATE,args,callback,extra);
}
