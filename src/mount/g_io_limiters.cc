#include "config.h"
#include "mount/g_io_limiters.h"

IoLimiter gIoLimiter;
ioLimiting::LimiterProxy& gGlobalIoLimiter() {
	static ioLimiting::MasterLimiter masterLimiter;
	static ioLimiting::RTClock clock;
	static ioLimiting::LimiterProxy limiter(masterLimiter, clock);
	return limiter;
}
