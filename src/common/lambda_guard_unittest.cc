#include "common/platform.h"
#include "common/lambda_guard.h"

#include <gtest/gtest.h>

TEST(LambdaGuardTests, LambdaGuard) {
	int value = 0;

	auto guard1 = makeLambdaGuard([&]() { value += 10; });
	auto guard2 = std::move(guard1);
	{
		auto guard3 = makeLambdaGuard([&]() { value += 1; });
		auto guard4 = std::move(guard3);
		// now guard4 calls 'value += 1'
		// now guard3 disappears
	}

	EXPECT_EQ(1, value);
	// now guard2 calls 'value += 10'
	// now guard1 disappears
}
