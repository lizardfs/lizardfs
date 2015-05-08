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
