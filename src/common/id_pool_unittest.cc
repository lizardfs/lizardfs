#include "common/platform.h"
#include "common/id_pool.h"

#include <gtest/gtest.h>

TEST(IdPoolTests, TestGet) {
	IdPool<uint32_t> pool(10);

	// Take exactly pool.size() IDs from the pool
	for (unsigned i = 0; i < pool.size(); ++i) {
		ASSERT_NO_THROW(pool.get()) << "n=" << i;
	}

	// Verify if no more IDs can be taken
	ASSERT_THROW(pool.get(), IdPoolException);
}

TEST(IdPoolTests, TestPut) {
	IdPool<uint32_t> pool(10);
	for (unsigned i = 0; i < 6 * pool.size(); ++i) {
		ASSERT_NO_THROW(pool.put(pool.get())) << "n=" << i;
	}
}

TEST(IdPoolTests, TestPutNull) {
	IdPool<uint32_t> pool(16);

	// Get all IDs from the pool
	try {
		while (true) {
			pool.get();
		}
	} catch (IdPoolException&) {
	}

	// Try to return the null ID
	ASSERT_THROW(pool.put(pool.nullId()), IdPoolException);
}

TEST(IdPoolTests, TestIfAllDifferent) {
	IdPool<uint32_t> pool(100);
	std::set<IdPool<uint32_t>::Id> takenIds;

	// Take all IDs and verify if all are different
	for (unsigned i = 0; i < pool.size(); ++i) {
		auto id = pool.get();
		ASSERT_EQ(0U, takenIds.count(id)) << "n=" << i;
		takenIds.insert(id);
	}

	// One by one, return Id to a pool, get a new one (the only one present there)
	// and verify if it is the expected one.
	for (unsigned i = 0; i < pool.size(); ++i) {
		auto someId = *(std::next(takenIds.begin(), i));
		ASSERT_FALSE(someId == pool.nullId());
		pool.put(someId);
		ASSERT_EQ(someId, pool.get());
	}
}
