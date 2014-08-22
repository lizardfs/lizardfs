#include "common/platform.h"
#include "mount/tweaks.h"

#include <gtest/gtest.h>

TEST(TweaksTests, GetAllValues) {
	std::atomic<uint32_t> u32(9);
	std::atomic<uint64_t> u64(19);
	std::atomic<bool> b(false);

	Tweaks t;
	t.registerVariable("u32", u32);
	t.registerVariable("u64", u64);
	t.registerVariable("bool", b);

	ASSERT_EQ("u32\t9\nu64\t19\nbool\tfalse\n", t.getAllValues());
}

TEST(TweaksTests, SetValue) {
	std::atomic<uint32_t> u32(9);
	std::atomic<uint64_t> u64(19);
	std::atomic<bool> b(false);

	Tweaks t;
	t.registerVariable("u32", u32);
	t.registerVariable("u64", u64);
	t.registerVariable("bool", b);

	t.setValue("u32", "   blah");
	t.setValue("u32", "blah");
	t.setValue("u32", "");
	t.setValue("u32", "\n");
	ASSERT_EQ(9U, u32);

	t.setValue("u32", "  16 xxx");
	ASSERT_EQ(16U, u32);

	t.setValue("u32", "15\n");
	ASSERT_EQ(15U, u32);

	t.setValue("u64", "150");
	ASSERT_EQ(15U, u32);
	ASSERT_EQ(150U, u64);
	ASSERT_FALSE(b.load());

	t.setValue("bool", "true");
	ASSERT_TRUE(b.load());
	t.setValue("bool", "false");
	ASSERT_FALSE(b.load());
	t.setValue("bool", "true\n");
	ASSERT_TRUE(b.load());
}
