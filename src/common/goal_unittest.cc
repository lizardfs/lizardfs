/*
   Copyright 2015 Skytechnology sp. z o.o.

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
#include "common/goal.h"

#include <gtest/gtest.h>

#define sneakyPartType(type) \
	Goal::Slice::Type(Goal::Slice::Type:: type)

TEST(GoalTests, BasicSliceOperations) {
	Goal::Slice std(Goal::Slice::Type(Goal::Slice::Type::kStandard));
	Goal::Slice xor3(Goal::Slice::Type(Goal::Slice::Type::kXor3));

	EXPECT_EQ(1, std.size());
	EXPECT_EQ(4, xor3.size());

	std[0][MediaLabel("A")] = 1;
	std[0][MediaLabel("B")] = 1;
	std[0][MediaLabel("_")] = 3;

	xor3[0][MediaLabel("A")] = 2;
	xor3[1][MediaLabel("B")] = 1;
	xor3[2][MediaLabel("C")] = 1;
	xor3[3][MediaLabel("B")] = 1;
	xor3[3][MediaLabel("_")] = 1;

	EXPECT_EQ(5, Goal::Slice::countLabels(std[0]));
	EXPECT_EQ(5, std.getExpectedCopies());
	EXPECT_EQ(6, xor3.getExpectedCopies());

	EXPECT_TRUE(std == std);
	EXPECT_FALSE(std == xor3);
	EXPECT_TRUE(std != xor3);

	EXPECT_EQ(Goal::Slice::Type(Goal::Slice::Type::kStandard), std.getType());
	EXPECT_EQ(Goal::Slice::Type(Goal::Slice::Type::kXor3), xor3.getType());

	EXPECT_EQ("$std {A B _ _ _}", to_string(std));
	EXPECT_EQ("$xor3 {{A A} B C {B _}}", to_string(xor3));
}

TEST(GoalTests, BasicSliceMerge) {
	Goal::Slice std_1(Goal::Slice::Type(Goal::Slice::Type::kStandard));
	Goal::Slice std_2(Goal::Slice::Type(Goal::Slice::Type::kStandard));
	Goal::Slice res(Goal::Slice::Type(Goal::Slice::Type::kStandard));

	std_1[0][MediaLabel("A")] = 1;
	std_1[0][MediaLabel("B")] = 1;
	std_1[0][MediaLabel("_")] = 3;

	std_2[0][MediaLabel("B")] = 2;
	std_2[0][MediaLabel("_")] = 2;

	res[0][MediaLabel("A")] = 1;
	res[0][MediaLabel("B")] = 2;
	res[0][MediaLabel("_")] = 2;

	std_1.mergeIn(std_2);

	EXPECT_EQ(std_1, res);
}

TEST(GoalTests, BasicXorMerge) {
	Goal::Slice x1(Goal::Slice::Type(Goal::Slice::Type::kXor2));
	Goal::Slice x2(Goal::Slice::Type(Goal::Slice::Type::kXor2));
	Goal::Slice res(Goal::Slice::Type(Goal::Slice::Type::kXor2));

	x1[0][MediaLabel("A")] = 1;
	x1[1][MediaLabel("B")] = 1;
	x1[2][MediaLabel("C")] = 1;

	x2[0][MediaLabel("C")] = 1;
	x2[1][MediaLabel("A")] = 1;
	x2[2][MediaLabel("B")] = 1;

	res[0][MediaLabel("A")] = 1;
	res[1][MediaLabel("B")] = 1;
	res[2][MediaLabel("C")] = 1;

	x1.mergeIn(x2);

	EXPECT_EQ(res, x1) << "result = " << to_string(x1);
}

TEST(GoalTests, BasicXorMerge2) {
	Goal::Slice x1(Goal::Slice::Type(Goal::Slice::Type::kXor2));
	Goal::Slice x2(Goal::Slice::Type(Goal::Slice::Type::kXor2));
	Goal::Slice res(Goal::Slice::Type(Goal::Slice::Type::kXor2));

	x1[0][MediaLabel("A")] = 1;
	x1[0][MediaLabel("_")] = 1;
	x1[1][MediaLabel("B")] = 1;
	x1[2][MediaLabel("C")] = 1;

	x2[0][MediaLabel("C")] = 1;
	x2[1][MediaLabel("D")] = 1;
	x2[2][MediaLabel("B")] = 1;

	res[0][MediaLabel("A")] = 1;
	res[0][MediaLabel("D")] = 1;
	res[1][MediaLabel("B")] = 1;
	res[2][MediaLabel("C")] = 1;

	x1.mergeIn(x2);

	EXPECT_EQ(res, x1) << "result = " << to_string(x1);
}

TEST(GoalTests, BasicXorMerge3) {
	Goal::Slice x1(Goal::Slice::Type(Goal::Slice::Type::kXor2));
	Goal::Slice x2(Goal::Slice::Type(Goal::Slice::Type::kXor2));
	Goal::Slice res(Goal::Slice::Type(Goal::Slice::Type::kXor2));

	x1[0][MediaLabel("A")] = 1;
	x1[0][MediaLabel("_")] = 1;
	x1[1][MediaLabel("B")] = 1;
	x1[2][MediaLabel("C")] = 1;

	x2[0][MediaLabel("D")] = 1;
	x2[1][MediaLabel("C")] = 1;
	x2[2][MediaLabel("B")] = 1;

	res[0][MediaLabel("A")] = 1;
	res[0][MediaLabel("D")] = 1;
	res[1][MediaLabel("B")] = 1;
	res[2][MediaLabel("C")] = 1;

	x1.mergeIn(x2);

	EXPECT_EQ(res, x1) << "result = " << to_string(x1);
}

TEST(GoalTests, BasicXorMerge4) {
	Goal::Slice x1(Goal::Slice::Type(Goal::Slice::Type::kXor3));
	Goal::Slice x2(Goal::Slice::Type(Goal::Slice::Type::kXor3));
	Goal::Slice res(Goal::Slice::Type(Goal::Slice::Type::kXor3));

	x1[0][MediaLabel("A")] = 1;
	x1[0][MediaLabel("_")] = 2;
	x1[1][MediaLabel("B")] = 7;
	x1[2][MediaLabel("C")] = 4;
	x1[3][MediaLabel("D")] = 5;

	x2[2][MediaLabel("A")] = 1;
	x2[2][MediaLabel("_")] = 2;
	x2[0][MediaLabel("B")] = 3;
	x2[3][MediaLabel("C")] = 7;
	x2[1][MediaLabel("D")] = 5;

	x1.mergeIn(x2);
	x1.mergeIn(x2);
	x1.mergeIn(x1);

	res[0][MediaLabel("A")] = 1;
	res[0][MediaLabel("_")] = 2;
	res[1][MediaLabel("B")] = 7;
	res[2][MediaLabel("C")] = 7;
	res[3][MediaLabel("D")] = 5;

	EXPECT_EQ(res, x1) << "result = " << to_string(x1);
}

TEST(GoalTests, GoalMerge) {
	Goal goal1("1");
	EXPECT_EQ(0, goal1.size());

	Goal::Slice slice11(Goal::Slice::Type(Goal::Slice::Type::kXor3));
	slice11[0][MediaLabel("B")] = 1;
	slice11[1][MediaLabel("C")] = 1;
	slice11[2][MediaLabel("_")] = 1;
	slice11[3][MediaLabel("A")] = 1;
	goal1.setSlice(slice11);
	EXPECT_EQ(1, goal1.size());

	Goal::Slice slice12(Goal::Slice::Type(Goal::Slice::Type::kStandard));
	slice12[0][MediaLabel("_")] = 1;
	slice12[0][MediaLabel("A")] = 1;
	goal1.setSlice(slice12);

	EXPECT_EQ(2, goal1.size());

	Goal goal2("2");
	EXPECT_EQ(0, goal2.size());

	Goal::Slice slice21(Goal::Slice::Type(Goal::Slice::Type::kXor2));
	slice21[0][MediaLabel("A")] = 1;
	slice21[1][MediaLabel("_")] = 1;
	slice21[2][MediaLabel("B")] = 1;
	goal2.setSlice(slice21);
	EXPECT_EQ(1, goal2.size());

	Goal::Slice slice22(Goal::Slice::Type(Goal::Slice::Type::kXor3));
	slice22[0][MediaLabel("C")] = 1;
	slice22[1][MediaLabel("_")] = 1;
	slice22[2][MediaLabel("A")] = 1;
	slice22[3][MediaLabel("B")] = 1;
	goal2.setSlice(slice22);
	EXPECT_EQ(2, goal2.size());

	goal2.mergeIn(goal1);
	EXPECT_EQ(3, goal2.size());

	EXPECT_EQ(goal2.end(), goal2.find(sneakyPartType(kXor4)));
	EXPECT_EQ(goal2.end(), goal2.find(sneakyPartType(kXor7)));
	EXPECT_EQ(goal2.end(), goal2.find(sneakyPartType(kTape)));

	EXPECT_NE(goal2.end(), goal2.find(sneakyPartType(kStandard)));
	EXPECT_NE(goal2.end(), goal2.find(sneakyPartType(kXor2)));
	EXPECT_NE(goal2.end(), goal2.find(sneakyPartType(kXor3)));
}
