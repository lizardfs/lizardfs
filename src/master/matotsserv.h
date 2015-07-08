/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include "common/id_pool.h"
#include "common/tape_key.h"
#include "common/tape_copy_location_info.h"
#include "common/network_address.h"

/// Type of objects which identify tapeservers.
typedef uint32_t TapeserverId;

/// Initialize the module.
int matotsserv_init();

/// Checks if a file can be enqueued to tapeserver
bool matotsserv_can_enqueue_node();

/// Enqueues file for sending it to tapeserver.
TapeserverId matotsserv_enqueue_node(const TapeKey& key);

/// Get an address of the given tapeserver.
/// \returns status
uint8_t matotsserv_get_tapeserver_info(TapeserverId id, TapeserverListEntry& tapeserverInfo);

/// Get vector of connected tapeservers.
std::vector<TapeserverListEntry> matotsserv_get_tapeservers();
