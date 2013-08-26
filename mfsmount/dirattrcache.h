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

#ifndef _DIRATTRCACHE_H_
#define _DIRATTRCACHE_H_

void dcache_init();
uint32_t dcache_replace(uint32_t parent,const uint8_t *dbuff,uint32_t dsize);
uint8_t dcache_getdir(uint32_t parent,const uint8_t **dbuff,uint32_t *dsize);
uint8_t dcache_lookup(uint32_t parent,uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[35]);
uint8_t dcache_getattr(uint32_t inode,uint8_t attr[35]);
uint8_t dcache_setattr(uint32_t parent,uint32_t inode,const uint8_t attr[35]);
uint8_t dcache_remove(uint32_t parent);
void dcache_remove_all();

#endif
