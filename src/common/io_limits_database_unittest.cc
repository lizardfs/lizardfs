#include "common/platform.h"
#include "common/io_limits_database.h"

#include <gtest/gtest.h>

#include "common/time_utils.h"

TEST(IoLimitsDatabaseTests, GetGroups) {
	IoLimitsDatabase db;
	db.setLimits(SteadyTimePoint(), {{"g1", 11}, {"g22", 22}, {"g333", 33}}, 1);
	std::vector<std::string> expectedGroups = {"g1", "g22", "g333"};
	ASSERT_EQ(db.getGroups(), expectedGroups);
}

TEST(IoLimitsDatabaseTests, Limiting) {
	SteadyTimePoint t0;
	IoLimitsDatabase db;
	db.setLimits(t0, {
			{"g1", 11},
			// This is dumb. 1000 stands for 1000 kilobytes per second, which
			// is 1000*1024 bytes per second:
			{"g22", 1000},
			{"g333", 33}}, 250);

	t0 += std::chrono::milliseconds(7);
	// 3 < 7, request can be fully served:
	ASSERT_EQ(3 * 1024U, db.request(t0, "g22", 3 * 1024U));

	// 6 > (7-3), only 4 kilobytes can be served:
	ASSERT_EQ(4 * 1024U, db.request(t0, "g22", 6 * 1024U));

	// Limit is exceeded, nothing can be served:
	ASSERT_EQ(0U, db.request(t0, "g22", 5 * 1024U));

	// After another 5 milliseconds another 5 kilobytes can be server:
	t0 += std::chrono::milliseconds(5);
	ASSERT_EQ(5 * 1024U, db.request(t0, "g22", 10200000U));
}

TEST(IoLimitsDatabaseTests, Accumulate) {
	SteadyTimePoint t0;

	IoLimitsDatabase db;
	db.setLimits(t0, {{"g22", 1000}}, 237);

	for (auto twice : {1, 2}) {
		SCOPED_TRACE("i: " + std::to_string(twice));
		// After many seconds have passed without any traffic..
		t0 += std::chrono::seconds(5);

		// ..We can still only distribute (1000KB * 237ms / 1s) bytes
		for (auto j = 0; j < 236; ++j) {
			SCOPED_TRACE("j: " + std::to_string(j));
			ASSERT_EQ(1024U, db.request(t0, "g22", 1024U));
		}
		// After 236 loops we only have 1KB left:
		ASSERT_EQ(1024U, db.request(t0, "g22", 1000000000000U));
		// And nothing more:
		ASSERT_EQ(0U, db.request(t0, "g22", 1U));

		// After another few milliseconds we can read another few kilobytes.
		// Let's test few few values:
		for (auto few = 1; few < 100; ++few) {
			SCOPED_TRACE("few: " + std::to_string(few));
			t0 += std::chrono::milliseconds(few);
			ASSERT_EQ(few * 1024U, db.request(t0, "g22", 1000000000000U));
		}
	}

}

