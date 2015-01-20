#pragma once

#include "common/platform.h"

#include "common/id_pool.h"

/// A pool of tapeserver IDs.
/// We support only 8-bit IDs to save some memory.
/// 84533 is a random number which uniquely identifies this pool.
typedef IdPool<uint8_t, 84533> TapeserverIdPool;

/// Type of objects which identify tapeservers.
typedef TapeserverIdPool::Id TapeserverId;

/// Initialize the module.
int matotsserv_init();
