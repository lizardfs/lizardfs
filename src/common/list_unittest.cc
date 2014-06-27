#include "common/platform.h"
#include "common/list.h"

#include <gtest/gtest.h>

struct SophisticatedList {
	char dummy1;
	uint32_t dummy2;
	SophisticatedList* next;
};

TEST(ListTests, ListLength) {
	SophisticatedList l1;
	EXPECT_EQ(0u, list_length((SophisticatedList*) NULL));
	l1.next = NULL;
	EXPECT_EQ(1u, list_length(&l1));
	SophisticatedList l2;
	l2.next = &l1;
	EXPECT_EQ(2u, list_length(&l2));
}
