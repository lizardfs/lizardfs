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

#ifndef _CSCOMM_H_
#define _CSCOMM_H_

int cs_readblock(int fd,uint64_t chunkid,uint32_t version,uint32_t offset,uint32_t size,uint8_t *buff);
/*
int cs_writestatus(int fd,uint64_t chunkid,uint32_t writeid);
int cs_writeinit(int fd,const uint8_t *chain,uint32_t chainsize,uint64_t chunkid,uint32_t version);
int cs_writeblock(int fd,uint64_t chunkid,uint32_t writeid,uint16_t blockno,uint16_t offset,uint32_t size,uint8_t *buff);
*/

#endif
