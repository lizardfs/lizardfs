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

#ifndef _MAIN_H_
#define _MAIN_H_

#include <poll.h>
#include <inttypes.h>

#define TIMEMODE_SKIP 0
#define TIMEMODE_RUNONCE 1
#define TIMEMODE_RUNALL 2
void main_destructregister (void (*fun)(void));
void main_canexitregister (int (*fun)(void));
void main_wantexitregister (void (*fun)(void));
void main_reloadregister (void (*fun)(void));
void main_pollregister (void (*desc)(struct pollfd *,uint32_t *),void (*serve)(struct pollfd *));
void main_eachloopregister (void (*fun)(void));
void main_timeregister (int mode,uint32_t seconds,uint32_t offset,void (*fun)(void));
int main_time(void);

#endif
