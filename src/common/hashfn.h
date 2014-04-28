/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013 Skytechnology sp. z o.o..

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

#include "config.h"

#include <inttypes.h>
#include <syslog.h>
#include <numeric>
#include <stdexcept>
#include <type_traits>

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

#define HASH_PRIMITIVE32(type) \
static inline uint64_t hash(type val) { \
	return hash32mult(val); \
}

#define HASH_PRIMITIVE64(type) \
static inline uint64_t hash(type val) { \
	return hash64(val); \
}

HASH_PRIMITIVE32(bool)
HASH_PRIMITIVE32(char)
HASH_PRIMITIVE32(signed char)
HASH_PRIMITIVE32(unsigned char)
HASH_PRIMITIVE32(short)
HASH_PRIMITIVE32(unsigned short)
HASH_PRIMITIVE32(int)
HASH_PRIMITIVE32(unsigned int)
HASH_PRIMITIVE64(long)
HASH_PRIMITIVE64(unsigned long)
HASH_PRIMITIVE64(long long)
HASH_PRIMITIVE64(unsigned long long)

// takes the hash, not an arbitrary object instance
static inline void hashCombineRaw(uint64_t& seed, uint64_t hash) {
	seed ^= hash + 11400714819323198485ULL + (seed << 6) + (seed >> 2);
}

static inline void hashCombine(uint64_t&) {
}

/// when a uint8_t* is followed by an integral type, assume it is a bytearray and its length
template<class T, class... Args, class = typename std::enable_if<std::is_integral<T>::value>::type>
static inline void hashCombine(uint64_t& seed, const uint8_t* str, T strLen, Args... args);

template<class T, class... Args>
static inline void hashCombine(uint64_t& seed, const T& val, Args... args) {
	hashCombineRaw(seed, hash(val));
	hashCombine(seed, args...);
}

/// doesn't modify its argument and returns result
template <typename T>
static inline uint64_t hashCombineReturn(uint64_t hash, const T& val) {
	hashCombine(hash, val);
	return hash;
}

/// hash byte array
static inline uint64_t hash(const uint8_t* str, uint32_t len) {
	const uint64_t init = 399871011; // arbitrary number
	return std::accumulate(str, str + len, init, hashCombineReturn<uint8_t>);
}

template<class T, class... Args, class>
static inline void hashCombine(uint64_t& seed, const uint8_t* str, T strLen, Args... args) {
	hashCombineRaw(seed, hash(str, strLen));
	hashCombine(seed, args...);
}

// functions for dynamically changing checksums
inline void addToChecksum(uint64_t& checksum, uint64_t hash) {
	checksum ^= hash;
}
inline void removeFromChecksum(uint64_t& checksum, uint64_t hash) {
	checksum ^= hash;
}
