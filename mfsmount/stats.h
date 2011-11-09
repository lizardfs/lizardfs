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

#ifndef _STATS_H_
#define _STATS_H_

#include <inttypes.h>

void* stats_get_subnode(void *node,const char *name,uint8_t absolute);
uint64_t* stats_get_counterptr(void *node);
void stats_reset_all(void);
void stats_show_all(char **buff,uint32_t *leng);
void stats_lock(void);
void stats_unlock(void);
void stats_term(void);

#endif
