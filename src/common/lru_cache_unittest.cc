#include "common/platform.h"
#include "common/lru_cache.h"

#include <future>
#include <memory>
#include <gtest/gtest.h>

#include "common/massert.h"

TEST(LruCacheTests, TestIfCacheUsedIndeed) {
	std::unique_ptr<LruCache<uint64_t, uint64_t>> fibonacciNumbers;
	SteadyTimePoint t0;
	SteadyDuration d0 = std::chrono::duration_cast<SteadyDuration>(std::chrono::seconds(123));
	auto maxElements = 1000000;

	int called = 0;
	int maxCalls = 500;
	auto fibonacciFunction = [&](uint64_t i) {
		sassert(called++ < maxCalls);
		if (i <= 2) {
			return uint64_t(1);
		} else {
			return fibonacciNumbers->get(t0, i - 1) + fibonacciNumbers->get(t0, i - 2);
		}
	};
	fibonacciNumbers.reset(new LruCache<uint64_t, uint64_t>(d0, maxElements, fibonacciFunction));

	ASSERT_NO_THROW(fibonacciNumbers->get(t0, 100));
	ASSERT_NO_THROW(fibonacciNumbers->get(t0, 500));
	ASSERT_NO_THROW(fibonacciNumbers->get(t0, 300));
	ASSERT_EQ(500, called);

	uint64_t fiftiethFibonacciNumber = 12586269025ull;
	ASSERT_EQ(fiftiethFibonacciNumber, fibonacciNumbers->get(t0, 50));
}

TEST(LruCacheTests, TestErase) {
	SteadyTimePoint t0;
	SteadyDuration d0 = std::chrono::duration_cast<SteadyDuration>(std::chrono::seconds(123));
	auto maxElements = 1000000;

	std::vector<uint64_t> functionArguments;
	auto fun = [&](uint64_t i) {
		functionArguments.push_back(i);
		return 2 * i;
	};

	LruCache<uint64_t, uint64_t> cache(d0, maxElements, fun);

	ASSERT_EQ(2u, cache.get(t0, 1));
	ASSERT_EQ(4u, cache.get(t0, 2));
	ASSERT_EQ(6u, cache.get(t0, 3));
	ASSERT_EQ(4u, cache.get(t0, 2));
	ASSERT_EQ(8u, cache.get(t0, 4));
	decltype(functionArguments) expectedArguments = {1, 2, 3, 4};
	ASSERT_EQ(expectedArguments, functionArguments);

	expectedArguments.clear();
	functionArguments.clear();

	cache.erase(1);
	cache.erase(2);
	cache.erase(2);
	cache.erase(2);

	ASSERT_EQ(2u, cache.get(t0, 1));
	ASSERT_EQ(4u, cache.get(t0, 2));
	expectedArguments = {1, 2};
	ASSERT_EQ(expectedArguments, functionArguments);

	ASSERT_EQ(2u, cache.get(t0, 1));
	ASSERT_EQ(4u, cache.get(t0, 2));
	ASSERT_EQ(expectedArguments, functionArguments);
}

TEST(LruCacheTests, TestMaxTime) {
	std::vector<uint64_t> functionArguments;
	auto fun = [&](uint64_t i) {
		functionArguments.push_back(i);
		return i;
	};
	LruCache<uint64_t, uint64_t> cache(std::chrono::seconds(5), 10000, fun);

	// We use cache that invalides entries after 5 seconds. Every second we will try
	// to read value from cache
	SteadyTimePoint t0;
	decltype(functionArguments) argumentsToBePassedToCacheGet =
			{0, 1, 2, 0, 1, 2, 3, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5};
	decltype(functionArguments) argumentsExpectedToBePassedToFunction =
			{0, 1, 2,          3, 0, 1, 2,    4,          3,    5};
	// Arguments that were not passed to function are these that were read in <= 5 seconds, so
	// can be served from cache

	for (auto arg : argumentsToBePassedToCacheGet) {
		t0 += std::chrono::seconds(1);
		ASSERT_EQ(arg, cache.get(t0, arg));
	}
	ASSERT_EQ(argumentsExpectedToBePassedToFunction, functionArguments);
}

TEST(LruCacheTests, TestMaxSize) {
	std::vector<uint64_t> functionArguments;
	auto fun = [&](uint64_t i) {
		functionArguments.push_back(i);
		return i;
	};

	uint64_t maxSize = 5;
	LruCache<uint64_t, uint64_t> cache(std::chrono::seconds(1000000), maxSize, fun);

	SteadyTimePoint t0;
	for (int i = 0; i < 2; ++i) {
		// The cache works OK if we read maxSize elements in any order:
		for (uint64_t j = 0; j < maxSize; ++j) {
			t0 += std::chrono::seconds(1);
			cache.get(t0, j);
		}
		decltype(functionArguments) argumentsExpectedToBePassedToFunction = {0, 1, 2, 3, 4};
		ASSERT_EQ(argumentsExpectedToBePassedToFunction, functionArguments);
	}

	// The cache works awfully (there aren't any cache hits) when we read maxSize+1 elements
	// in a loop:
	for (uint64_t j = maxSize; j < 10 * maxSize; ++j) {
		uint64_t arg = j % (maxSize + 1); // maxSize, 0, 1, 2, ..., maxSize - 1, maxSize, 0, 1, 2, ...

		t0 += std::chrono::seconds(1);
		functionArguments.clear();
		cache.get(t0, arg);
		ASSERT_EQ(1u, functionArguments.size());
		ASSERT_EQ(arg, functionArguments[0]);
	}
}

TEST(LruCacheTests, TestMultiThreadedCache) {
	auto fun = [](uint64_t i) {
		return i;
	};
	LruCacheMt<uint64_t, uint64_t> cache(std::chrono::seconds(1000000), 100, fun);
	SteadyTimePoint t0;

	std::vector<std::future<void>> asyncs;
	for (int i  = 0; i < 10; i++) {
		asyncs.push_back(
				std::async(std::launch::async, [&]()
				{
					for (uint64_t j = 0; j < 1000; ++j) {
						ASSERT_EQ(j, cache.get(t0, j));
						ASSERT_NO_THROW(cache.erase(j));
						ASSERT_NO_THROW(cache.cleanup(t0, 2));
					}
				}));
	}
}
