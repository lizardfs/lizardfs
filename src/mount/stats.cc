/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2019 Skytechnology sp. z o.o.

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
#include "mount/stats.h"

#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

statsnode* stats_get_subnode(statsnode *node, const char *name, uint8_t absolute) {
	stats_lock();

	statsnode *a = node ? node->firstchild : firstnode;

	for (; a; a = a->nextsibling)
		if (!strcmp(a->name, name))
			goto unlock_stats_and_exit;

	if (!(a = (statsnode*)malloc(sizeof(statsnode))))
		goto unlock_stats_and_exit;

	a->nextsibling = node ? node->firstchild : firstnode;
	a->firstchild = NULL;
	a->counter = 0;
	a->active = 0;
	a->absolute = absolute;
	a->name = strdup(name);
	a->nleng = strlen(name);

	if (node) {
		a->fnleng = node->fnleng + 1 + a->nleng;

		char *bstr;
		if (!(bstr = (char*)malloc(a->fnleng + 1))) {
			free(a);
			a = NULL;
			goto unlock_stats_and_exit;
		}

		memcpy(bstr, node->fullname, node->fnleng);
		bstr[node->fnleng] = '.';
		memcpy(bstr + node->fnleng + 1, a->name, a->nleng);
		bstr[a->fnleng] = 0;
		a->fullname = bstr;
	} else {
		a->fullname = a->name;
		a->fnleng = a->nleng;
	}

	if (node)
		node->firstchild = a;
	else
		firstnode = a;

unlock_stats_and_exit:
	stats_unlock();
	return a;
}

uint64_t* stats_get_counterptr(statsnode *node) {
	stats_lock();

	if (!node->active) {
		node->active = 1;
		allactiveplengs += node->fnleng;
		activenodes++;
	}

	stats_unlock();

	return &(node->counter);
}

static inline void stats_reset(statsnode *node) {
	statsnode *a;

	if (!node->absolute)
		node->counter = 0;

	for (a = node->firstchild; a; a = a->nextsibling)
		stats_reset(a);
}

void stats_reset_all(void) {
	statsnode *a;
	stats_lock();

	for (a = firstnode; a; a = a->nextsibling)
		stats_reset(a);

	stats_unlock();
}

static inline uint32_t stats_print_values(char *buff, uint32_t maxleng, statsnode *n) {
	uint32_t l = n->active ? snprintf(buff, maxleng, "%s: %" PRIu64 "\n", n->fullname, n->counter) : 0;

	for (statsnode *a = n->firstchild; a; a = a->nextsibling)
		if (maxleng > l)
			l += stats_print_values(buff + l, maxleng - l, a);

	return l;
}

static inline uint32_t stats_print_total(char *buff, uint32_t maxleng) {
	uint32_t l = 0;

	for (statsnode *a = firstnode; a; a = a->nextsibling)
		if (maxleng > l)
			l += stats_print_values(buff + l, maxleng - l, a);

	return l;
}

void stats_show_all(char **buff, uint32_t *leng) {
	stats_lock();

	uint32_t rl = allactiveplengs + 23 * activenodes + 1;
	*buff = (char*) malloc(rl);
	*leng = *buff ? stats_print_total(*buff,rl) : 0;

	stats_unlock();
}

void stats_free(statsnode *n) {
	statsnode *a, *an;
	free(n->name);

	if (n->fullname != n->name)
		free(n->fullname);

	for (a = n->firstchild; a; a = an) {
		an = a->nextsibling;
		stats_free(a);
		free(a);
	}
}

void stats_term(void) {
	statsnode *a, *an;

	for (a = firstnode; a; a = an) {
		an = a->nextsibling;
		stats_free(a);
		free(a);
	}
}
