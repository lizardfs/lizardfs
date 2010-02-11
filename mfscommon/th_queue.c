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
#include <pthread.h>
#include <inttypes.h>

#include "th_sem.h"

typedef struct _qentry {
	uint32_t id;
	uint32_t op;
	uint8_t *data;
	uint32_t leng;
	struct _qentry *next;
} qentry;

typedef struct _queue {
	qentry *head,**tail;
	void *semfree,*semfull;
	pthread_mutex_t lock;
} queue;

void* queue_new(uint32_t size) {
	queue *q;
	q = (queue*)malloc(sizeof(queue));
	q->head = NULL;
	q->tail = &(q->head);
	if (size==0) {
		q->semfull = NULL;
	} else {
		q->semfull = sem_new(size);
	}
	q->semfree = sem_new(0);
	pthread_mutex_init(&(q->lock),NULL);
	return q;
}

void queue_delete(void *que) {
	queue *q = (queue*)que;
	qentry *qe,*qen;
	for (qe = q->head ; qe ; qe = qen) {
		qen = qe->next;
		free(qe->data);
		free(qe);
	}
	sem_delete(q->semfree);
	if (q->semfull) {
		sem_delete(q->semfull);
	}
	pthread_mutex_destroy(&(q->lock));
	free(q);
}

int queue_isempty(void *que) {
	queue *q = (queue*)que;
	return (sem_getresamount(q->semfree)==0);
}

uint32_t queue_elements(void *que) {
	queue *q = (queue*)que;
	return sem_getresamount(q->semfree);
}

int queue_isfull(void *que) {
	queue *q = (queue*)que;
	if (q->semfull) {
		return (sem_getresamount(q->semfull)==0);
	}
	return 0;
}

uint32_t queue_sizeleft(void *que) {
	queue *q = (queue*)que;
	if (q->semfull) {
		return sem_getresamount(q->semfull);
	}
	return 0xFFFFFFFF;
}

void queue_put(void *que,uint32_t id,uint32_t op,uint8_t *data,uint32_t leng) {
	queue *q = (queue*)que;
	qentry *qe;
	qe = malloc(sizeof(qentry));
	qe->id = id;
	qe->op = op;
	qe->data = data;
	qe->leng = leng;
	qe->next = NULL;
	if (q->semfull) {
		sem_acquire(q->semfull,leng);
	}
	pthread_mutex_lock(&(q->lock));
	*(q->tail) = qe;
	q->tail = &(qe->next);
	pthread_mutex_unlock(&(q->lock));
	sem_release(q->semfree,1);
}

int queue_tryput(void *que,uint32_t id,uint32_t op,uint8_t *data,uint32_t leng) {
	queue *q = (queue*)que;
	qentry *qe;
	if (q->semfull && sem_tryacquire(q->semfull,leng)<0) {
		return -1;
	}
	qe = malloc(sizeof(qentry));
	qe->id = id;
	qe->op = op;
	qe->data = data;
	qe->leng = leng;
	qe->next = NULL;
	pthread_mutex_lock(&(q->lock));
	*(q->tail) = qe;
	q->tail = &(qe->next);
	pthread_mutex_unlock(&(q->lock));
	sem_release(q->semfree,1);
	return 0;
}

void queue_get(void *que,uint32_t *id,uint32_t *op,uint8_t **data,uint32_t *leng) {
	queue *q = (queue*)que;
	qentry *qe;
	sem_acquire(q->semfree,1);
	pthread_mutex_lock(&(q->lock));
	qe = q->head;
	q->head = qe->next;
	if (q->head==NULL) {
		q->tail = &(q->head);
	}
	pthread_mutex_unlock(&(q->lock));
	if (q->semfull) {
		sem_release(q->semfull,qe->leng);
	}
	if (id) {
		*id = qe->id;
	}
	if (op) {
		*op = qe->op;
	}
	if (data) {
		*data = qe->data;
	}
	if (leng) {
		*leng = qe->leng;
	}
	free(qe);
}

int queue_tryget(void *que,uint32_t *id,uint32_t *op,uint8_t **data,uint32_t *leng) {
	queue *q = (queue*)que;
	qentry *qe;
	if (sem_tryacquire(q->semfree,1)<0) {
		if (id) {
			*id=0;
		}
		if (op) {
			*op=0;
		}
		if (data) {
			*data=NULL;
		}
		if (leng) {
			*leng=0;
		}
		return -1;
	}
	pthread_mutex_lock(&(q->lock));
	qe = q->head;
	q->head = qe->next;
	if (q->head==NULL) {
		q->tail = &(q->head);
	}
	pthread_mutex_unlock(&(q->lock));
	if (q->semfull) {
		sem_release(q->semfull,qe->leng);
	}
	if (id) {
		*id = qe->id;
	}
	if (op) {
		*op = qe->op;
	}
	if (data) {
		*data = qe->data;
	}
	if (leng) {
		*leng = qe->leng;
	}
	free(qe);
	return 0;
}
