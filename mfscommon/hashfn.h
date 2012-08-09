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

#ifndef _HASHFN_H_
#define _HASHFN_H_

#include <inttypes.h>

/* fast integer hash functions by Thomas Wang */
/* all of them pass the avalanche test */

/* They are not mutch better in standard collision test than stupid "X*prime"
 * functions, but calculation times are similar, so it should be safer to use
 * this functions */

/* 32bit -> 32bit */
static inline uint32_t hash32(uint32_t key) {
	key = ~key + (key << 15); // key = (key << 15) - key - 1;
	key = key ^ (key >> 12);
	key = key + (key << 2);
	key = key ^ (key >> 4);
	key = key * 2057; // key = (key + (key << 3)) + (key << 11);
	key = key ^ (key >> 16);
	return key;
}

/* 32bit -> 32bit - with 32bit multiplication (can be adjusted by other constant values, such as: 0xb55a4f09,0x165667b1,2034824023,2034824021 etc.) */
static inline uint32_t hash32mult(uint32_t key) { 
	key = (key ^ 61) ^ (key >> 16); 
	key = key + (key << 3); 
	key = key ^ (key >> 4); 
	key = key * 0x27d4eb2d; 
	key = key ^ (key >> 15); 
	return key; 
}

/* 64bit -> 32bit */
static inline uint32_t hash6432(uint64_t key) {
	key = (~key) + (key << 18); // key = (key << 18) - key - 1;
	key = key ^ (key >> 31);
	key = key * 21; // key = (key + (key << 2)) + (key << 4);
	key = key ^ (key >> 11);
	key = key + (key << 6);
	key = key ^ (key >> 22);
	return (uint32_t)key;
}

/* 64bit -> 64bit */
static inline uint64_t hash64(uint64_t key) {
	key = (~key) + (key << 21); // key = (key << 21) - key - 1;
	key = key ^ (key >> 24);
	key = (key + (key << 3)) + (key << 8); // key * 265
	key = key ^ (key >> 14);
	key = (key + (key << 2)) + (key << 4); // key * 21
	key = key ^ (key >> 28);
	key = key + (key << 31);
	return key;
}

#endif
