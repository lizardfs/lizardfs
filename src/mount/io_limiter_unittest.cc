#include "mount/io_limiter.h"

#include <unistd.h>
#include <gtest/gtest.h>

#include "common/time_utils.h"

TEST(IoLimitQueue, WaitNoReserve) {
	IoLimitQueue queue;
	queue.setLimit(100);

	Timer timer;
	queue.wait(1024);
	ASSERT_NEAR(timer.elapsed_ms(), 10, 3);
	queue.wait(1024);
	ASSERT_NEAR(timer.elapsed_ms(), 20, 3);
	queue.wait(1024);
	ASSERT_NEAR(timer.elapsed_ms(), 30, 3);
}

TEST(IoLimitQueue, WaitWithReserve) {
	IoLimitQueue queue;
	queue.setLimit(100);
	usleep(30000); // sleep 3/100 of a second to allow us to read 3 kB without waiting

	Timer timer;
	queue.wait(1024);
	ASSERT_NEAR(timer.elapsed_ms(), 0, 3);
	queue.wait(1024);
	ASSERT_NEAR(timer.elapsed_ms(), 0, 3);
	queue.wait(1024);
	ASSERT_NEAR(timer.elapsed_ms(), 0, 3);
}

TEST(IoLimitQueueCollection, CreateAndGetQueue) {
	IoLimitQueueCollection limiter;
	ASSERT_NO_THROW(limiter.createQueue("q1", 100));
	ASSERT_NO_THROW(limiter.createQueue("q12", 100));
	ASSERT_THROW(limiter.createQueue("q12", 50), WrongIoLimitQueueException);

	ASSERT_NO_THROW(limiter.getQueue("q1").setLimit(10));
	ASSERT_NO_THROW(limiter.getQueue("q12").setLimit(50));
	ASSERT_THROW(limiter.getQueue("q").wait(130), WrongIoLimitQueueException);
	ASSERT_THROW(limiter.getQueue("q").wait(30), WrongIoLimitQueueException);
}
