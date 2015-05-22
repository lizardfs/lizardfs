#pragma once

#include "common/platform.h"

#include "common/id_pool.h"
#include "common/tape_key.h"
#include "common/tape_copy_location_info.h"
#include "common/network_address.h"

/// A pool of tapeserver IDs.
/// We support only 8-bit IDs to save some memory.
/// 84533 is a random number which uniquely identifies this pool.
typedef IdPool<uint8_t, 84533> TapeserverIdPool;

/// Type of objects which identify tapeservers.
typedef TapeserverIdPool::Id TapeserverId;

/// Initialize the module.
int matotsserv_init();

/// Enqueues file for sending it to tapeserver.
TapeserverId matotsserv_enqueue_node(const TapeKey& key);

/// Get an address of the given tapeserver.
/// \returns status
uint8_t matotsserv_get_tapeserver_info(TapeserverId id, TapeserverListEntry& tapeserverInfo);

/// Get vector of connected tapeservers.
std::vector<TapeserverListEntry> matotsserv_get_tapeservers();
