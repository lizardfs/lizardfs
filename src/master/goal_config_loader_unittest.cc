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

// Helper macros to test the loader
#define LABELS(...) (std::vector<std::string>({__VA_ARGS__}))
#define EXPECT_GOAL(loader, expected_id, expected_name, expected_labels, expected_tape_labels) \
	EXPECT_EQ(expected_name, loader.goals()[expected_id].name()); \
	EXPECT_EQ(ExpectedLabels(expected_labels), loader.goals()[expected_id].chunkLabels()); \
	EXPECT_EQ(ExpectedLabels(expected_tape_labels), loader.goals()[expected_id].tapeLabels()); \
	EXPECT_EQ(expected_labels.size(), loader.goals()[expected_id].getExpectedCopies());


Goal::Labels ExpectedLabels(const std::vector<std::string>& tokens) {
	Goal::Labels labels;
	for (const auto& token : tokens) {
		++labels[MediaLabel(token)];
	}
	return labels;
}

TEST(GoalConfigLoaderTests, Defaults) {
	GoalConfigLoader loader;
	ASSERT_NO_THROW(loader.load(std::istringstream("")));
	std::vector<std::string> labels;
	for (int goal = goal::kMinOrdinaryGoal; goal <= goal::kMaxOrdinaryGoal; ++goal) {
		SCOPED_TRACE("Testing default value for goal " + ::testing::PrintToString(goal));
		labels.push_back(MediaLabelManager::kWildcard); // add one label
		EXPECT_GOAL(loader, goal, std::to_string(goal), labels, {});
	}
}

TEST(GoalConfigLoaderTests, CorrectFile) {
	// A macro to make the test shorter
	#define ANY MediaLabelManager::kWildcard

	std::string config(
			"# a comment \n"
			"1  tmp:       ssd    # temporary files only on SSDs!\n"
			"     # another comment\n"
			"\n"
			"10 fast     :   _ ssd\n"
			"11 safe     :   _ local remote\n"
			"12 fast_safe:   local ssd remote\n"
			"15 blahbah: _    _   hdd\n"
			"16 tape2: _@    _   tape@\n"
			"17 tape_aaa: _ _ aaa@\n"
			"18 only_tape: _@ _@ _@"

	);
	GoalConfigLoader loader;
	ASSERT_NO_THROW(loader.load(std::istringstream(config)));
	EXPECT_GOAL(loader, 1,  "tmp",       LABELS("ssd"),                    LABELS());
	EXPECT_GOAL(loader, 10, "fast",      LABELS(ANY, "ssd"),               LABELS());
	EXPECT_GOAL(loader, 11, "safe",      LABELS(ANY, "local", "remote"),   LABELS());
	EXPECT_GOAL(loader, 12, "fast_safe", LABELS("local", "ssd", "remote"), LABELS());
	EXPECT_GOAL(loader, 16, "tape2",     LABELS(ANY),                      LABELS(ANY, "tape"));
	EXPECT_GOAL(loader, 17, "tape_aaa",  LABELS(ANY, ANY),                 LABELS("aaa"));
	EXPECT_GOAL(loader, 18, "only_tape", LABELS(),                         LABELS(ANY, ANY, ANY));

	#undef ANY
}

TEST(GoalConfigLoaderTests, IncorrectLines) {
	GoalConfigLoader loader;
	#define TRY_PARSE(s) loader.load(std::istringstream(s "\n"))

	// Malformed lines
	EXPECT_THROW(TRY_PARSE("1"), ParseException);
	EXPECT_THROW(TRY_PARSE("goal : _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("1    : goal _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 lone name of goal: media1 media2"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal: # nothing here"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 goal :ssd"), ParseException);

	// Invalid values
	EXPECT_THROW(TRY_PARSE("0 goal : _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("21 goal : _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("-1 goal : _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 one/two : one two"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 one : one/two"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 one : one.two"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 one@ : _ _"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : @"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : @@"), ParseException);
	EXPECT_THROW(TRY_PARSE("1 1 : aaa@aa@"), ParseException);

	// duplicates
	EXPECT_THROW(TRY_PARSE("1 1: _\n2 2: _ _\n2: 3 _ _"), ParseException);

	#undef TRY_PARSE
}
