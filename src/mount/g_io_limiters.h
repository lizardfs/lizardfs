#pragma once

#include "config.h"

#include "mount/io_limiter.h"
#include "mount/global_io_limiter.h"

extern IoLimiter gIoLimiter;
ioLimiting::LimiterProxy& gGlobalIoLimiter();
