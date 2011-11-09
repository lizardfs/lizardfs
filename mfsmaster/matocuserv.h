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

#ifndef _MATOCUSERV_H_
#define _MATOCUSERV_H_

#include <inttypes.h>

void matocuserv_stats(uint64_t stats[5]);

void matocuserv_notify_attr(uint32_t dirinode,uint32_t inode,const uint8_t attr[35]);
void matocuserv_notify_link(uint32_t dirinode,uint8_t nleng,const uint8_t *name,uint32_t inode,const uint8_t attr[35],uint32_t ts);
void matocuserv_notify_unlink(uint32_t dirinode,uint8_t nleng,const uint8_t *name,uint32_t ts);
void matocuserv_notify_remove(uint32_t dirinode);
void matocuserv_notify_parent(uint32_t dirinode,uint32_t parent);

void matocuserv_chunk_status(uint64_t chunkid,uint8_t status);
void matocuserv_init_sessions(uint32_t sessionid,uint32_t inode);
int matocuserv_sessionsinit(void);
int matocuserv_networkinit(void);

#endif
