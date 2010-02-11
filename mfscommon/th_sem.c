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

typedef struct _semaphore {
	uint32_t count;
	pthread_mutex_t lock;
	pthread_cond_t cond;
} semaphore;


void* sem_new(uint32_t resources) {
	semaphore *s;
	s = (semaphore*)malloc(sizeof(semaphore));
	s->count = resources;
	pthread_mutex_init(&(s->lock),NULL);
	pthread_cond_init(&(s->cond),NULL);
	return s;
}

void sem_delete(void *sem) {
	semaphore *s = (semaphore*)sem;
	pthread_mutex_destroy(&(s->lock));
	pthread_cond_destroy(&(s->cond));
	free(s);
}

uint32_t sem_getresamount(void *sem) {
	semaphore *s = (semaphore*)sem;
	uint32_t res;
	pthread_mutex_lock(&(s->lock));
	res = s->count;
	pthread_mutex_unlock(&(s->lock));
	return res;
}

void sem_acquire(void *sem,uint32_t res) {
	semaphore *s = (semaphore*)sem;
	pthread_mutex_lock(&(s->lock));
	while (s->count<res) {
		pthread_cond_wait(&(s->cond),&(s->lock));
	}
	s->count-=res;
	pthread_mutex_unlock(&(s->lock));
}

int sem_tryacquire(void *sem,uint32_t res) {
	semaphore *s = (semaphore*)sem;
	pthread_mutex_lock(&(s->lock));
	if (s->count<res) {
		pthread_mutex_unlock(&(s->lock));
		return -1;
	}
	s->count-=res;
	pthread_mutex_unlock(&(s->lock));
	return 0;
}

void sem_release(void *sem,uint32_t res) {
	semaphore *s = (semaphore*)sem;
	pthread_mutex_lock(&(s->lock));
	s->count+=res;
	pthread_mutex_unlock(&(s->lock));
	pthread_cond_signal(&(s->cond));
}

void sem_broadcast_release(void *sem,uint32_t res) {
	semaphore *s = (semaphore*)sem;
	pthread_mutex_lock(&(s->lock));
	s->count+=res;
	pthread_mutex_unlock(&(s->lock));
	pthread_cond_broadcast(&(s->cond));
}
