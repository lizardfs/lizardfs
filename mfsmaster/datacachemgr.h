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

#ifndef _DATACACHEMGR_H_
#define _DATACACHEMGR_H_

#include <inttypes.h>

int dcm_open(uint32_t inode,uint32_t sessionid);
void dcm_access(uint32_t inode,uint32_t sessionid);
void dcm_modify(uint32_t inode,uint32_t sessionid);
int dcm_init(void);

#endif
