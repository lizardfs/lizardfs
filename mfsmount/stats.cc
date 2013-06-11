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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <inttypes.h>

typedef struct _statsnode {
	uint64_t counter;
	uint8_t active;
	uint8_t absolute;
	char *name;
	char *fullname;
	uint32_t nleng;	// : strlen(name)
	uint32_t fnleng; // : strlen(fullname)
	struct _statsnode *firstchild;
	struct _statsnode *nextsibling;
} statsnode;

static statsnode *firstnode = NULL;
static uint32_t allactiveplengs = 0;
static uint32_t activenodes = 0;
static pthread_mutex_t glock = PTHREAD_MUTEX_INITIALIZER;

void stats_lock(void) {
	pthread_mutex_lock(&glock);
}

void stats_unlock(void) {
	pthread_mutex_unlock(&glock);
}

void* stats_get_subnode(void *node,const char *name,uint8_t absolute) {
	statsnode *sn = (statsnode*)node;
	statsnode *a;
	pthread_mutex_lock(&glock);
	for (a=sn?sn->firstchild:firstnode ; a ; a=a->nextsibling) {
		if (strcmp(a->name,name)==0) {
			pthread_mutex_unlock(&glock);
			return a;
		}
	}
	a = malloc(sizeof(statsnode));
	a->nextsibling = sn?sn->firstchild:firstnode;
	a->firstchild = NULL;
	a->counter = 0;
	a->active = 0;
	a->absolute = absolute;
	a->name = strdup(name);
	a->nleng = strlen(name);
	if (sn) {
		char *bstr;
		a->fnleng = sn->fnleng+1+a->nleng;
		bstr = malloc(a->fnleng+1);
		memcpy(bstr,sn->fullname,sn->fnleng);
		bstr[sn->fnleng]='.';
		memcpy(bstr+sn->fnleng+1,a->name,a->nleng);
		bstr[a->fnleng]=0;
		a->fullname = bstr;
	} else {
		a->fullname = a->name;
		a->fnleng = a->nleng;
	}
	if (sn) {
		sn->firstchild = a;
	} else {
		firstnode = a;
	}
	pthread_mutex_unlock(&glock);
	return a;
}

uint64_t* stats_get_counterptr(void *node) {
	statsnode *sn = (statsnode*)node;
	pthread_mutex_lock(&glock);
	if (sn->active==0) {
		sn->active = 1;
		allactiveplengs += sn->fnleng;
		activenodes++;
	}
	pthread_mutex_unlock(&glock);
	return &(sn->counter);
}

static inline void stats_reset(statsnode *n) {
	statsnode *a;
	if (n->absolute==0) {
		n->counter = 0;
	}
	for (a=n->firstchild ; a ; a=a->nextsibling) {
		stats_reset(a);
	}
}

void stats_reset_all(void) {
	statsnode *a;
	pthread_mutex_lock(&glock);
	for (a=firstnode ; a ; a=a->nextsibling) {
		stats_reset(a);
	}
	pthread_mutex_unlock(&glock);
}

static inline uint32_t stats_print_values(char *buff,uint32_t maxleng,statsnode *n) {
	statsnode *a;
	uint32_t l;
	if (n->active) {
		l = snprintf(buff,maxleng,"%s: %"PRIu64"\n",n->fullname,n->counter);
	} else {
		l = 0;
	}
	for (a=n->firstchild ; a ; a=a->nextsibling) {
		if (maxleng>l) {
			l += stats_print_values(buff+l,maxleng-l,a);
		}
	}
	return l;
}

static inline uint32_t stats_print_total(char *buff,uint32_t maxleng) {
	statsnode *a;
	uint32_t l;
	l = 0;
	for (a=firstnode ; a ; a=a->nextsibling) {
		if (maxleng>l) {
			l += stats_print_values(buff+l,maxleng-l,a);
		}
	}
	return l;
}

void stats_show_all(char **buff,uint32_t *leng) {
	uint32_t rl;
	pthread_mutex_lock(&glock);
	rl = allactiveplengs + 23*activenodes + 1;
	*buff = malloc(rl);
	if (*buff) {
		*leng = stats_print_total(*buff,rl);
	} else {
		*leng = 0;
	}
	pthread_mutex_unlock(&glock);
}

void stats_free(statsnode *n) {
	statsnode *a,*an;
	free(n->name);
	if (n->fullname != n->name) {
		free(n->fullname);
	}
	for (a=n->firstchild ; a ; a = an) {
		an = a->nextsibling;
		stats_free(a);
		free(a);
	}
}

void stats_term(void) {
	statsnode *a,*an;
	for (a=firstnode ; a ; a = an) {
		an = a->nextsibling;
		stats_free(a);
		free(a);
	}
}
