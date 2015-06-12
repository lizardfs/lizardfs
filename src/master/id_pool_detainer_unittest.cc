#include "common/platform.h"
#include "master/id_pool_detainer.h"

#include <gtest/gtest.h>

TEST(IdPoolDetainerTests, TestGet) {
	IdPoolDetainer<uint32_t, uint32_t> pool(1000, 10, 100, 100, 64, 10);

	// Take exactly pool.size() IDs from the pool
	for (unsigned i = 0; i < pool.maxSize(); ++i) {
		EXPECT_EQ(pool.acquire(), i + 1) << "n=" << i;
	}

	// Verify if no more IDs can be taken
	EXPECT_EQ(pool.acquire(), (uint32_t)0);
}

TEST(IdPoolDetainerTests, TestPut) {
	IdPoolDetainer<uint32_t, uint32_t> pool(1000, 10, 100, 100, 64, 10);

	for (unsigned i = 0; i < 6 * pool.maxSize(); ++i) {
		ASSERT_TRUE(pool.release(pool.acquire(), 10)) << "n=" << i;
	}
}

TEST(IdPoolDetainerTests, TestIdIsNull) {
	IdPoolDetainer<uint32_t, uint32_t> pool(1000, 10, 1000, 100, 64, 10);

	ASSERT_EQ(pool.nullId, (uint32_t)0);

	for (unsigned i = 0; i < pool.maxSize(); ++i) {
		ASSERT_TRUE(pool.acquire() != 0) << "n=" << i;
	}
}

TEST(IdPoolDetainerTests, TestPutNull) {
	IdPoolDetainer<uint32_t, uint32_t> pool(1000, 10, 128, 128, 64, 10);

	// Get all IDs from the pool
	while (pool.acquire()) {
	}

	// Try to return the null ID
	ASSERT_FALSE(pool.release(0, 10));
}

TEST(IdPoolDetainerTests, TestIfAllDifferent) {
	IdPoolDetainer<uint32_t, uint32_t> pool(1000, 10, 130, 130, 64, 0);
	std::set<uint32_t> takenIds;

	// Take all IDs and verify if all are different
	for (unsigned i = 0; i < pool.maxSize(); ++i) {
		auto id = pool.acquire();
		ASSERT_EQ(0U, takenIds.count(id)) << "n=" << i;
		takenIds.insert(id);
	}

	// One by one, return Id to a pool, get a new one (the only one present there)
	// and verify if it is the expected one.
	for (unsigned i = 0; i < pool.maxSize(); ++i) {
		auto someId = *(std::next(takenIds.begin(), i));
		ASSERT_FALSE(someId == 0);
		ASSERT_TRUE(pool.release(someId, 10));
		ASSERT_EQ(someId, pool.acquire());
	}
}

TEST(IdPoolDetainerTests, TestMarkAsAcquired) {
	IdPoolDetainer<uint32_t, uint32_t> pool(1000, 10, 5000, 5000, 128, 30);

	pool.markAsAcquired(300);
	pool.markAsAcquired(4100);
	pool.markAsAcquired(5000);

	while (1) {
		uint32_t id = pool.acquire();
		EXPECT_TRUE(id != 300 && id != 4100 && id != 5000) << "id=" << id;
		if (!id) {
			break;
		}
	}
}

TEST(IdPoolDetainerTests, TestIfDetained) {
	IdPoolDetainer<uint32_t, uint32_t> pool(1000, 10, 200, 200, 64, 0);
	std::set<uint32_t> takenIds;

	// take 50 ids
	for (unsigned i = 0; i < 50; ++i) {
		auto id = pool.acquire();
		ASSERT_EQ(0U, takenIds.count(id)) << "n=" << i;
		takenIds.insert(id);
	}

	// release all 50 (they should go to detention) at time 10
	for (auto &id : takenIds) {
		ASSERT_TRUE(pool.release(id, 10));
	}

	EXPECT_EQ(50U, pool.detainedCount());

	// take rest of the ids and check if they aren't equal to detained
	for (unsigned i = 0; i < 150; ++i) {
		auto id = pool.acquire();
		EXPECT_EQ(0U, takenIds.count(id)) << "n=" << i;
	}

	EXPECT_EQ(50U, pool.detainedCount());
	EXPECT_EQ(150U, pool.size());

	std::set<uint32_t> detainedIds;
	for(const auto& entry: pool) {
		ASSERT_EQ(0U, detainedIds.count(entry.id)) << "n=" << entry.id;
		detainedIds.insert(entry.id);
	}

	EXPECT_EQ(takenIds,detainedIds);

	pool.releaseDetained(1200,100);

	EXPECT_EQ(0U, pool.detainedCount());
	EXPECT_EQ(150U, pool.size());
}
