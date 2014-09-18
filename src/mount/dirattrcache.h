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

#include "common/platform.h"

#include <inttypes.h>

#include "mount/lizard_client_context.h"

/// Creates a new entry from the result of readdir.
void* dcache_new(const LizardClient::Context *ctx,
		uint32_t parent,const uint8_t *dbuff,uint32_t dsize);

/// Invalidate cache for the given directory.
/// Invalidates all existing cache entries for the given directory. Use after removing
/// any entry from \p parent (negative lookups are not cached, so creating new files
/// doesn't require this).
void dcache_invalidate(uint32_t parent);

/// Frees cache entry.
void dcache_release(void *r);

/// Lookup in the cache.
/// Returns a non-zero value if a valid entry for the given (ctx, parent, name) tuple is found
/// and fills (inode, attr).
uint8_t dcache_lookup(const LizardClient::Context *ctx,uint32_t parent,
		uint8_t nleng,const uint8_t *name,uint32_t *inode,uint8_t attr[35]);

uint8_t dcache_getattr(const LizardClient::Context *ctx,uint32_t inode,uint8_t attr[35]);
