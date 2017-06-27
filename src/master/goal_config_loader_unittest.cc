/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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
#include "master/goal_config_loader.h"

#include <gtest/gtest.h>

#include "common/exceptions.h"
#include "common/media_label.h"
#include "common/slice_traits.h"

// Helper macros to test the loader
#define LABELS(...) (std::vector<std::string>({__VA_ARGS__}))

#define EXPECT_GOAL(goals, expected_id, expected_name, expected_slice) \
	{ \
		EXPECT_EQ(1, goals[(expected_id)].size()); \
		EXPECT_EQ((expected_name), goals[(expected_id)].getName()); \
		EXPECT_EQ((expected_slice), *(goals[(expected_id)].begin())); \
	}

typedef decltype(goal_config::load(std::istringstream(""))) Goals;

Goal::Slice createSlice(int type,
		std::vector<std::map<std::string, int>> part_list) {
	Goal::Slice slice{Goal::Slice::Type(type)};
	int part_index = 0;
	for (const auto &part : part_list) {
		for (const auto label : part) {
			slice[part_index][MediaLabel(label.first)] += label.second;
		}
		++part_index;
	}
	return slice;
}

TEST(GoalConfigTests, Defaults) {
	Goals goals;
	ASSERT_NO_THROW(goals = goal_config::load(std::istringstream("")));
	for (int goal_id = GoalId::kMin; goal_id <= GoalId::kMax; ++goal_id) {
		SCOPED_TRACE("Testing default value for goal " + ::testing::PrintToString(goal_id));
		EXPECT_GOAL(goals, goal_id, std::to_string(goal_id),
				createSlice(Goal::Slice::Type::kStandard,
						{{{"_", std::min(goal_id, goal_config::kMaxCompatibleGoal)}}}));
	}
}

TEST(GoalConfigTests, OldFormat) {
	std::string config(
			"# a comment \n"
			"1  tmp:       ssd    # temporary files only on SSDs!\n"
			"     # another comment\n"
			"\n"
			"10 fast     :   _ ssd\n"
			"11 safe     :   _ local remote\n"
			"12 fast_safe:   local ssd remote\n"
			"15 blahbah: _    _   hdd\n"
			"16 tape2: _    _   tape\n"
			"17 AAABB: A B B A A\n"
			"18 3: _ _ _"

	);
	Goals goals;
	ASSERT_NO_THROW(goals = goal_config::load(std::istringstream(config)));
	EXPECT_GOAL(goals, 1,  "tmp",       createSlice(Goal::Slice::Type::kStandard, {
			{{"ssd", 1}}}));
	EXPECT_GOAL(goals, 10, "fast",      createSlice(Goal::Slice::Type::kStandard, {
			{{"_", 1}, {"ssd", 1}}}));
	EXPECT_GOAL(goals, 11, "safe",      createSlice(Goal::Slice::Type::kStandard, {
			{{"_", 1}, {"local", 1}, {"remote", 1}}}));
	EXPECT_GOAL(goals, 12, "fast_safe", createSlice(Goal::Slice::Type::kStandard, {
			{{"local", 1}, {"ssd", 1}, {"remote", 1}}}));
	EXPECT_GOAL(goals, 15, "blahbah",   createSlice(Goal::Slice::Type::kStandard, {
			{{"_", 2}, {"hdd", 1}}}));
	EXPECT_GOAL(goals, 16, "tape2",     createSlice(Goal::Slice::Type::kStandard, {
			{{"_", 2}, {"tape", 1}}}));
	EXPECT_GOAL(goals, 17, "AAABB",      createSlice(Goal::Slice::Type::kStandard, {
			{{"B", 2}, {"A", 3}}}));
	EXPECT_GOAL(goals, 18, "3",      createSlice(Goal::Slice::Type::kStandard, {
			{{"_", 3}}}));

}

TEST(GoalConfigTests, NewFormat) {
	std::string config(
			"# a comment \n"
			"1  tmp:    $      std  {    ssd}    # temporary files only on SSDs!\n"
			"     # another comment\n"
			"\n"
			"10 fast     :  $ std{ _ ssd}\n"
			"11 safe     :  $std {    _ local remote}\n"
			"12 fast_safe:  $std {local ssd remote}\n"
			"15 xor2: $xor2\n"
			"16 x5: $xor5 { A B C }\n"
			"17 AAABB: $ xor4 {A B B A A}\n"
			"18 3: $xor3 {_ _ _}\n"
			"19 erasure1: $ec(3,4) {A A A C C}\n"
			"20 erasure2: $ec(2,2)\n"

	);
	Goals goals;
	ASSERT_NO_THROW(goals = goal_config::load(std::istringstream(config)));
	EXPECT_GOAL(goals, 1,  "tmp",       createSlice(Goal::Slice::Type::kStandard, {
			{{"ssd", 1}}}));
	EXPECT_GOAL(goals, 10, "fast",      createSlice(Goal::Slice::Type::kStandard, {
			{{"_", 1}, {"ssd", 1}}}));
	EXPECT_GOAL(goals, 11, "safe",      createSlice(Goal::Slice::Type::kStandard, {
			{{"_", 1}, {"local", 1}, {"remote", 1}}}));
	EXPECT_GOAL(goals, 12, "fast_safe", createSlice(Goal::Slice::Type::kStandard, {
			{{"local", 1}, {"ssd", 1}, {"remote", 1}}}));
	EXPECT_GOAL(goals, 15, "xor2",   createSlice(Goal::Slice::Type::kXor2, {
			{{"_", 1}},
			{{"_", 1}},
			{{"_", 1}}}));
	EXPECT_GOAL(goals, 16, "x5",     createSlice(Goal::Slice::Type::kXor5, {
			{{"A", 1}},
			{{"B", 1}},
			{{"C", 1}},
			{{"_", 1}},
			{{"_", 1}},
			{{"_", 1}}}));
	EXPECT_GOAL(goals, 17, "AAABB",      createSlice(Goal::Slice::Type::kXor4, {
			{{"A", 1}},
			{{"B", 1}},
			{{"B", 1}},
			{{"A", 1}},
			{{"A", 1}}}));
	EXPECT_GOAL(goals, 18, "3",      createSlice(Goal::Slice::Type::kXor3, {
			{{"_", 1}},
			{{"_", 1}},
			{{"_", 1}},
			{{"_", 1}}}));
	EXPECT_GOAL(goals, 19, "erasure1", createSlice(int(slice_traits::ec::getSliceType(3, 4)), {
			{{"A", 1}},
			{{"A", 1}},
			{{"A", 1}},
			{{"C", 1}},
			{{"C", 1}},
			{{"_", 1}},
			{{"_", 1}}}));
	EXPECT_GOAL(goals, 20, "erasure2", createSlice(int(slice_traits::ec::getSliceType(2, 2)), {
			{{"_", 1}},
			{{"_", 1}},
			{{"_", 1}},
			{{"_", 1}}}));

}

TEST(GoalConfigTests, IncorrectLines) {
	#define TRY_PARSE(s) goal_config::load(std::istringstream(s "\n"))

	// Malformed lines
	EXPECT_THROW(TRY_PARSE("1"), ParseException);
	EXPECT_THROW(TRY_PARSE("goal : _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("1    : goal _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 lone name of goal: media1 media2"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal: # nothing here"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal:"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal: $std{}"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal :{ssd}"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal : $xor2{"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal : $xor2{A A A A}"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal : $ unknown"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal : $std unknown"), ParseException);

	// Invalid values
	EXPECT_THROW(TRY_PARSE("0 goal : _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("41 goal : _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("-1 goal : _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 one/two : one two"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 one : one/two"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 one : one.two"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 one@ : _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : @"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : @@"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : aaa@aa@"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ec(34,19)"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ec(19)"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ec"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ec(1,1) {A B C D E F}"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ec(3,2) {A B C D E F}"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ec (3,2)"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ec() {A B C}"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ex(2,2) {A B C}"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ex(2,2) {A B C}"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : (4,3) {A B C}"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ec(1,1)"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ec(33,1)"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ec(3,0)"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : $ec(3,33)"), ParseException);
	// duplicates
	EXPECT_THROW(TRY_PARSE("1 1: _\n2 2: _ _\n2: 3 _ _"), ParseException);

	#undef TRY_PARSE
}
