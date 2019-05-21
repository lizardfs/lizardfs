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

#pragma once

#include "common/platform.h"

#include <inttypes.h>

struct statsnode {
	uint64_t counter;
	uint8_t active;
	uint8_t absolute;
	char *name;
	char *fullname;
	uint32_t nleng; // : strlen(name)
	uint32_t fnleng; // : strlen(fullname)
	struct statsnode *firstchild;
	struct statsnode *nextsibling;
};

statsnode* stats_get_subnode(statsnode *node, const char *name, uint8_t absolute);
uint64_t* stats_get_counterptr(statsnode *node);
void stats_reset_all(void);
void stats_show_all(char **buff, uint32_t *leng);
void stats_lock(void);
void stats_unlock(void);
void stats_term(void);
