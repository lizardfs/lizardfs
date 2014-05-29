#include "config.h"
#include "mount/global_io_limiter.h"

#include <atomic>
#include <future>
#include <math.h>
#include <gtest/gtest.h>

#include "common/cltoma_communication.h"
#include "common/io_limit_group.h"
#include "common/io_limits_database.h"
#include "common/matocl_communication.h"
#include "unittests/packet.h"

using namespace ioLimiting;

/**
 * Many tests in this file are rather integration tests then unit tests.
 * The good side of it is that here we can test the limiting mechanism more precisely then
 * functional tests can.
 */

// Common implementation of callReconfigure for all limiters used in tests
struct TestingLimiter : public Limiter {
	void callReconfigure(uint32_t delta_us, const std::string& subsystem,
			const std::vector<std::string>& groups) {
		reconfigure_(delta_us, subsystem, groups);
	}
};

// This limiter simply accepts all requests
struct UnlimitedLimiter : public TestingLimiter {
	uint64_t request(const IoLimitGroupId&, uint64_t size) override {
		return size;
	}
};

// A clock that is incremented manually. 'sleepUntil' terminates when time is set to a value
// exceeding expected time
class ManuallyAdjustedClock : public Clock {
public:
	// maxLiveTime can be used to avoid infinite wait in case of 'sleepUntil' that doesn't
	// finish properly
	ManuallyAdjustedClock(SteadyDuration maxLiveTime = std::chrono::seconds(3))
			: timeout_(maxLiveTime) {
	}
	SteadyTimePoint now() override {
		std::unique_lock<std::mutex> lock(mutex_);
		return now_;
	}
	void sleepUntil(SteadyTimePoint time) override {
		std::unique_lock<std::mutex> lock(mutex_);
		while ((time > now_) && !timeout_.expired()) {
			cond_.wait_for(lock, timeout_.remainingTime());
		}
		if (timeout_.expired()) {
			abort();
		}
	}
	void increase(SteadyDuration duration) {
		std::unique_lock<std::mutex> lock(mutex_);
		sassert(duration >= std::chrono::nanoseconds(0));
		now_ += duration;
		cond_.notify_all();
	}
private:
	std::mutex mutex_;
	SteadyTimePoint now_;
	std::condition_variable cond_;
	Timeout timeout_;
};

// A clock that is simply incremented after every sleep
class FastClock : public Clock {
public:
	SteadyTimePoint now() override {
		std::unique_lock<std::mutex> lock(mutex_);
		return now_;
	}
	void sleepUntil(SteadyTimePoint time) override {
		std::unique_lock<std::mutex> lock(mutex_);
		sassert(now_ <= time);
		now_ = time;
	}
private:
	SteadyTimePoint now_;
	std::mutex mutex_;
};

// Limiter similar to the one used by real master
struct IoLimitsDatabaseLimiter : public TestingLimiter {
	IoLimitsDatabaseLimiter(Clock& clock) : clock_(clock), requestsNr_(0) {}
	uint64_t request(const IoLimitGroupId& groupId, uint64_t size) override {
		std::unique_lock<std::mutex> lock(mutex_);
		requestsNr_++;
		uint64_t grant = database.request(clock_.now(), groupId, size);
		return grant;
	}
	// Number of requests sent to the limiter:
	uint32_t requestsNr() {
		return requestsNr_;
	}
	IoLimitsDatabase database;
private:
	Clock& clock_;
	std::mutex mutex_;
	uint32_t requestsNr_;
};

// Check if the 'deadline' parameter of the 'wait' method works as expected
TEST(LimiterGroupTests, GroupDeadline) {
	UnlimitedLimiter limiter;
	SharedState shared{limiter, std::chrono::seconds(1)};
	FastClock clock;
	Group group(shared, clock);

	std::mutex mutex;
	std::unique_lock<std::mutex> lock(mutex);

	ASSERT_EQ(ETIMEDOUT, group.wait("group", 1, clock.now() - std::chrono::seconds(1), lock));
	ASSERT_EQ(ETIMEDOUT, group.wait("group", 1, clock.now(), lock));
	ASSERT_EQ(STATUS_OK, group.wait("group", 1, clock.now() + std::chrono::seconds(1), lock));
}

// Check if the request is handled without any sleeps when the limit is not reached
TEST(LimiterGroupTests, NoSleepWhenLimitNotReached) {
	UnlimitedLimiter limiter;
	SharedState shared{limiter, std::chrono::seconds(1)};
	FastClock clock;
	SteadyTimePoint testBegin = clock.now();
	Group group(shared, clock);

	std::mutex mutex;
	std::unique_lock<std::mutex> lock(mutex);

	auto loops = 1234;
	for (int i = 0; i < loops; i++) {
		group.wait("group", 1, SteadyTimePoint() + SteadyDuration(std::chrono::seconds(1)), lock);
	}
	ASSERT_EQ(testBegin, clock.now());
}

// Check if the throughput is properly limited during a reconfiguration
TEST(LimiterGroupTests, ThroughputChangeAfterReconfiguration) {
	const int N = 20;
	ManuallyAdjustedClock clock;
	IoLimitsDatabaseLimiter limiter(clock);
	limiter.database.setLimits(clock.now(), {{"group", 0/*KBps*/}}, 250);

	SharedState shared{limiter, std::chrono::milliseconds(1)};
	Group group(shared, clock);

	std::mutex mutex;

	std::atomic<int> total(0);
	std::condition_variable someoneFinished;
	std::vector<std::future<void>> asyncs;

	// Run N*3 1MB operation in parallel. The limit is set to 0Mbps, so all will hang
	for (int i  = 0; i < N*3; i++) {
		asyncs.push_back(
				std::async(std::launch::async, [&group, &total, &clock, &mutex, &someoneFinished]()
				{
					std::unique_lock<std::mutex> lock(mutex);
					uint8_t status = group.wait("group",
							1024 * 1024/*1MB*/, clock.now() + std::chrono::seconds(10), lock);
					ASSERT_EQ(STATUS_OK, status);
					total++;
					someoneFinished.notify_all();
				}));
	}

	// Set limit to 1MB per millisecond (1024*1000KBps), run the test for N milliseconds:
	limiter.database.setLimits(clock.now(), {{"group", 1024 * 1000}}, 250);
	for (int i = 1; i <= N; i++) {
		SCOPED_TRACE("i: " + std::to_string(i));
		clock.increase(std::chrono::milliseconds(1));
		// Expect that after each millisecond exactly one 1MB operation will succeed:
		std::unique_lock<std::mutex> lock(mutex);
		someoneFinished.wait(lock, [&total, i]() {return total >= i;});
		ASSERT_EQ(i, total);
	}

	// Raise the limit to 2MB per millisecond, run the test for next N milliseconds:
	limiter.database.setLimits(clock.now(), {{"group", 2 * 1024 * 1000}}, 250);
	for (int i = 1; i <= N; i++) {
		SCOPED_TRACE("i: " + std::to_string(i));
		clock.increase(std::chrono::milliseconds(1));
		// Expect that after each millisecond exactly two 1MB operations will succeed:
		std::unique_lock<std::mutex> lock(mutex);
		someoneFinished.wait(lock, [&total, i]() {return total >= N + 2*i;});
		ASSERT_EQ(N + 2*i, total);
	}
}

TEST(LimiterGroupTests, Die) {
	ManuallyAdjustedClock clock;
	IoLimitsDatabaseLimiter limiter(clock);
	limiter.database.setLimits(clock.now(), {{"group", 1000}}, 250);
	SharedState shared{limiter, std::chrono::seconds(100)};
	Group group(shared, clock);

	std::mutex mutex;

	// Reading works...
	{
		std::unique_lock<std::mutex> lock(mutex);
		clock.increase(std::chrono::seconds(1));
		ASSERT_EQ(STATUS_OK, group.wait("group", 1, clock.now() + std::chrono::seconds(1), lock));
	}
	group.die(); // .. but not after 'die' is called:
	{
		std::unique_lock<std::mutex> lock(mutex);
		clock.increase(std::chrono::seconds(1));
		ASSERT_EQ(ENOENT, group.wait("group", 1, clock.now() + std::chrono::seconds(1), lock));
	}
}

// It would be nice to provide this test, but we don't have any cgroup mock:
// TEST(LimiterProxyTests, GroupRemoved)

// Check if multiple parallel operations terminate after expected time for various parameter
// combinations
TEST(LimiterProxyTests, EndTimeAfterManyParallelReads) {
	for (int delta_us : {1, 567, 89012}) {
		SCOPED_TRACE("delta_us: " + std::to_string(delta_us));

		for (const int N : {1, 5}) {
			SCOPED_TRACE("N: " + std::to_string(N));

			for (const int M : {1, 7}) {
				SCOPED_TRACE("M: " + std::to_string(M));

				FastClock clock;
				IoLimitsDatabaseLimiter limiter(clock);
				limiter.database.setLimits(clock.now(), {{kUnclassified, 1000}}, 250);
				LimiterProxy lp(limiter, clock);
				limiter.callReconfigure(delta_us, "", {kUnclassified});

				auto beginTime = clock.now();
				auto deadline = beginTime + std::chrono::seconds(100);

				// run N jobs, each reading M kilobytes:
				std::vector<std::future<uint8_t>> statuses;
				for (int i = 0; i < N; ++i) {
					statuses.push_back(std::async(std::launch::async, [&lp, &deadline, M]()
							{return lp.waitForRead(getpid(), M * 1024, deadline);}));
				}
				// wait for them to finish:
				for (auto& status : statuses) {
					ASSERT_EQ(STATUS_OK, status.get());
				}

				// Check if they finished after a sane time. The limit is 1KBps, so we expect that
				// roughly N * M milliseconds have passed:
				auto time_passed_us = std::chrono::duration_cast<std::chrono::microseconds>(
						clock.now() - beginTime).count();
				auto rough_expected_time_us = N * M * 1000;
				ASSERT_NEAR(rough_expected_time_us, time_passed_us,
						std::max<int>(delta_us, rough_expected_time_us / 10));

				// Moreover, check if we can compute the exact read time:
				auto bytes_to_read = N * M * 1024;
				auto bytes_per_delta_us = int64_t((1024 * delta_us) / 1000);
				auto periods_needed_to_read_all = ceil(double(bytes_to_read) / bytes_per_delta_us);
				int64_t expected_read_time_us = delta_us * periods_needed_to_read_all;
				ASSERT_EQ(expected_read_time_us, time_passed_us);
			}
		}
	}
}

// Check if the throughput is properly limited when more then one mountpoint is simultaneously used
TEST(LimiterProxyTests, ManyMountsSummaryThroughput) {
	ManuallyAdjustedClock clock;
	IoLimitsDatabaseLimiter limiter(clock);
	limiter.database.setLimits(clock.now(), {{kUnclassified, 1000/*1000KBps*/}}, 1);

	auto beginTime = clock.now();
	auto deadline = beginTime + std::chrono::seconds(100);

	// Create a number of 'mountpoints' and set a proper configuration for all of them:
	LimiterProxy lp1(limiter, clock);
	limiter.callReconfigure(1000, "", {kUnclassified});
	LimiterProxy lp2(limiter, clock);
	limiter.callReconfigure(1000, "", {kUnclassified});
	LimiterProxy lp3(limiter, clock);
	limiter.callReconfigure(1000, "", {kUnclassified});

	// Mounts with number of threads that we'll run on these mount:
	auto limiterProxyWithThreadNumber = {
		std::make_pair<LimiterProxy*, int>(&lp1, 11),
		std::make_pair<LimiterProxy*, int>(&lp2, 13),
		std::make_pair<LimiterProxy*, int>(&lp3, 15)
	};

	std::mutex mutex;
	std::atomic<int> completed(0);
	std::atomic<int> expectedToBeCompleted(0);
	std::condition_variable someoneCompleted;
	std::vector<std::future<void>> asyncs;

	// Run N threads on every mount (aka proxyLimiter), each trying to read 1KB
	const int M = 17;
	for (auto& lpwtn : limiterProxyWithThreadNumber) {
		auto& proxyLimiter = *lpwtn.first;
		const int N = lpwtn.second;

		// Run N threads using LimiterProxy, each performing M subsequent reads
		for (int i  = 0; i < N; i++) {
			asyncs.push_back(std::async(std::launch::async, [&proxyLimiter, M, &deadline, &mutex,
					&someoneCompleted, &completed, &expectedToBeCompleted]() {
						for (auto i = 0; i < M; ++i) {
							ASSERT_EQ(STATUS_OK, proxyLimiter.waitForRead(
									getpid(), 1024/*1KB*/, deadline));
							std::unique_lock<std::mutex> lock(mutex);
							completed++;
							// Check if the number of threads that completed their 'waitForRead'
							// call matches the expected value
							ASSERT_EQ(completed, expectedToBeCompleted);
							someoneCompleted.notify_all();
						}
					}));
		}
	}

	// Tick the clock asyncs.size() times. Expect that after every tick exactly one thread will
	// terminate
	for (auto lpwtn : limiterProxyWithThreadNumber) {
		const int N = lpwtn.second;
		for (int i  = 0; i < (N * M); i++) {
			++expectedToBeCompleted;
			clock.increase(std::chrono::milliseconds(1));
			// Expect that after each millisecond exactly one 1KB operation will succeed:
			std::unique_lock<std::mutex> lock(mutex);
			someoneCompleted.wait(lock, [&completed, &expectedToBeCompleted]() {
					return completed >= expectedToBeCompleted;});
			ASSERT_EQ(expectedToBeCompleted, completed);
		}
	}
}

// Check if we don't communicate with the master too often
TEST(LimiterProxyTests, NumberOfRequestesSentToMaster) {
	for (auto delta_ms : {1, 11, 37, 128, 5678}) {
		SCOPED_TRACE("delta_ms: " + std::to_string(delta_ms));

		for (auto N : {1, 77, 129}) {
			SCOPED_TRACE("N: " + std::to_string(N));

			ManuallyAdjustedClock clock;
			IoLimitsDatabaseLimiter limiter(clock);
			limiter.database.setLimits(clock.now(), {{kUnclassified, 1000/*1000KBps*/}}, 1000);

			auto beginTime = clock.now();
			auto deadline = beginTime + std::chrono::seconds(100);

			// Create a number of 'mountpoints' and set a proper configuration for all of them:
			LimiterProxy lp(limiter, clock);
			limiter.callReconfigure(delta_ms * 1000, "", {kUnclassified});

			std::condition_variable cond;
			std::atomic<int> completed(0);
			std::mutex mutex;
			std::vector<std::future<void>> asyncs;

			// Run N operation in parallel:
			for (int i  = 0; i < N; i++) {
				asyncs.push_back(
						std::async(std::launch::async, [&lp, &deadline, &cond, &mutex, &completed]()
						{
							ASSERT_EQ(STATUS_OK, lp.waitForRead(getpid(), 1024/*1KB*/, deadline));
							completed++;
							std::unique_lock<std::mutex> lock(mutex);
							cond.notify_all();
						}));
			}
			usleep(1000);
			for (int i = 1; i <= ceil(double(N) / delta_ms); i++) {
				clock.increase(std::chrono::milliseconds(delta_ms));
				std::unique_lock<std::mutex> lock(mutex);
				cond.wait(lock, [&]{return completed == std::min(i * delta_ms, N);});
			}
			ASSERT_NEAR(ceil(double(N) / delta_ms), limiter.requestsNr(), 2);
		}
	}
}
