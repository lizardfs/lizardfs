#pragma once

#include "common/platform.h"

#include "common/io_limiting.h"
#include "mount/global_io_limiter.h"

ioLimiting::MountLimiter& gMountLimiter();
ioLimiting::LimiterProxy& gLocalIoLimiter();
ioLimiting::LimiterProxy& gGlobalIoLimiter();
