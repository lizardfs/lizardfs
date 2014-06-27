#include "common/platform.h"
#include "mount/g_io_limiters.h"

#include "mount/global_io_limiter.h"

ioLimiting::MountLimiter& gMountLimiter() {
	static ioLimiting::MountLimiter limiter;
	return limiter;
}

ioLimiting::LimiterProxy& gLocalIoLimiter() {
	static ioLimiting::RTClock clock;
	static ioLimiting::LimiterProxy limiter(gMountLimiter(), clock);
	return limiter;
}

ioLimiting::LimiterProxy& gGlobalIoLimiter() {
	static ioLimiting::MasterLimiter masterLimiter;
	static ioLimiting::RTClock clock;
	static ioLimiting::LimiterProxy limiter(masterLimiter, clock);
	return limiter;
}
