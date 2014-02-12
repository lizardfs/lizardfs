#include "limit_queue.h"

#include <gtest/gtest.h>

#define IDS(...) std::vector<LimitQueue::Id>({__VA_ARGS__})

TEST(LimitQueueTest, WithoutReserve) {
	LimitQueue queue(0, 0);
	ASSERT_EQ(IDS(), queue.pop(10));
	queue.push(0, 4);
	ASSERT_EQ(IDS(), queue.pop(3));
	ASSERT_EQ(IDS(0), queue.pop(7));
	queue.push(1, 4);
	queue.push(2, 4);
	queue.push(3, 4);
	ASSERT_EQ(IDS(1, 2), queue.pop(10));
	ASSERT_EQ(IDS(), queue.pop(1));
	ASSERT_EQ(IDS(3), queue.pop(1));
	ASSERT_EQ(IDS(), queue.pop(50));
}

TEST(LimitQueueTest, BeginReserve) {
	LimitQueue queue(4, 4);
	queue.push(0, 1);
	queue.push(1, 2);
	queue.push(2, 3);
	queue.push(3, 4);
	queue.push(4, 5);
	queue.push(5, 5);
	queue.push(6, 1);
	ASSERT_EQ(IDS(0, 1), queue.pop(0));
	ASSERT_EQ(IDS(), queue.pop(1));
	ASSERT_EQ(IDS(2), queue.pop(1));
	ASSERT_EQ(IDS(3), queue.pop(7));
	ASSERT_EQ(IDS(4, 5), queue.pop(7));
	ASSERT_EQ(IDS(6), queue.pop(1));
}

TEST(LimitQueueTest, MaxReserve) {
	LimitQueue queue(0, 4);
	queue.push(0, 1);
	ASSERT_EQ(IDS(0), queue.pop(10));
	queue.push(1, 5);
	ASSERT_EQ(IDS(), queue.pop(0));
	ASSERT_EQ(IDS(1), queue.pop(1));
}

TEST(LimitQueueTest, SetMaxReserve) {
	LimitQueue queue(0, 2);
	ASSERT_EQ(IDS(), queue.pop(4));
	queue.push(0, 4);
	ASSERT_EQ(IDS(), queue.pop(0));
	queue.setMaxReserve(4);
	ASSERT_EQ(IDS(0), queue.pop(6));
	queue.push(1, 4);
	ASSERT_EQ(IDS(1), queue.pop(0));
}
