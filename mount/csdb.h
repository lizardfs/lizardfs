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

#ifndef _CSDB_H_
#define _CSDB_H_

#include <inttypes.h>

void csdb_init(void);
void csdb_term(void);
uint32_t csdb_getreadcnt(uint32_t ip,uint16_t port);
uint32_t csdb_getwritecnt(uint32_t ip,uint16_t port);
uint32_t csdb_getopcnt(uint32_t ip,uint16_t port);
void csdb_readinc(uint32_t ip,uint16_t port);
void csdb_readdec(uint32_t ip,uint16_t port);
void csdb_writeinc(uint32_t ip,uint16_t port);
void csdb_writedec(uint32_t ip,uint16_t port);

#endif
