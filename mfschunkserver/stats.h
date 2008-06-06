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

#ifndef _STATS_H_
#define _STATS_H_

#include <inttypes.h>

uint32_t stats_datasize(uint32_t number);
void stats_makedata(uint8_t *buff,uint32_t number);
uint32_t stats_gifsize(uint32_t number);
void stats_makegif(uint8_t *buff);

int stats_init (void);

#endif
