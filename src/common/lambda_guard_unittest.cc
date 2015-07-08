/*
   Copyright 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

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
