#include "common/platform.h"
#include "common/id_pool.h"

#include <gtest/gtest.h>

TEST(IdPoolTests, TestGet) {
	IdPool<uint32_t> pool(100, 64, 10);

	// Take exactly pool.size() IDs from the pool
	for (unsigned i = 0; i < pool.maxSize(); ++i) {
		EXPECT_EQ(pool.acquire(), i + 1) << "n=" << i;
	}

	// Verify if no more IDs can be taken
	EXPECT_EQ(pool.acquire(), (uint32_t)0);
}

TEST(IdPoolTests, TestPut) {
	IdPool<uint32_t> pool(100, 64, 10);

	for (unsigned i = 0; i < 6 * pool.maxSize(); ++i) {
		ASSERT_TRUE(pool.release(pool.acquire())) << "n=" << i;
	}
}

TEST(IdPoolTests, TestIdIsNull) {
	IdPool<uint32_t> pool(1000, 64, 10);

	ASSERT_EQ(pool.nullId,(uint32_t)0);

	for (unsigned i = 0; i < pool.maxSize(); ++i) {
		ASSERT_TRUE(pool.acquire()!=0) << "n=" << i;
	}
}

TEST(IdPoolTests, TestPutNull) {
	IdPool<uint32_t> pool(128, 64, 10);

	// Get all IDs from the pool
	while (pool.acquire()) {
	}

	// Try to return the null ID
	ASSERT_FALSE(pool.release(0));
}

TEST(IdPoolTests, TestIfAllDifferent) {
	IdPool<uint32_t> pool(130, 64, 0);
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
		ASSERT_TRUE(pool.release(someId));
		ASSERT_EQ(someId,pool.acquire());
	}
}

TEST(IdPoolTests, TestMarkAsAcquired) {
	IdPool<uint32_t> pool(5000, 128, 30);

	pool.markAsAcquired(300);
	pool.markAsAcquired(4100);
	pool.markAsAcquired(4999);

	while (1) {
		uint32_t id = pool.acquire();
		EXPECT_TRUE(id != 300 && id != 4100 && id != 4999) << "id=" << id;
		if (!id) {
			break;
		}
	}
}
