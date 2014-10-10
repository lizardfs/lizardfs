#include "common/platform.h"
#include "master/goal_config_loader.h"

#include <gtest/gtest.h>

#include "common/exceptions.h"
#include "common/media_label.h"

// Helper macros to test the loader
#define LABELS(...) (std::vector<MediaLabel>({__VA_ARGS__}))
#define EXPECT_GOAL(loader, expected_id, expected_name, expected_labels) \
	EXPECT_EQ(expected_name, loader.goals()[expected_id].name()); \
	EXPECT_EQ(ExpectedLabels(expected_labels), loader.goals()[expected_id].labels()); \
	EXPECT_EQ(expected_labels.size(), loader.goals()[expected_id].getExpectedCopies());


Goal::Labels ExpectedLabels(const std::vector<MediaLabel>& tokens) {
	Goal::Labels labels;
	for (const auto& token : tokens) {
		++labels[token];
	}
	return labels;
}

TEST(GoalConfigLoaderTests, Defaults) {
	GoalConfigLoader loader;
	ASSERT_NO_THROW(loader.load(std::istringstream("")));
	std::vector<MediaLabel> labels;
	for (int goal = goal::kMinGoal; goal <= goal::kMaxGoal; ++goal) {
		SCOPED_TRACE("Testing default value for goal " + ::testing::PrintToString(goal));
		labels.push_back(kMediaLabelWildcard); // add one label
		EXPECT_GOAL(loader, goal, std::to_string(goal), labels);
	}
}

TEST(GoalConfigLoaderTests, CorrectFile) {
	// A macro to make the test shorter
	#define ANY kMediaLabelWildcard

	std::string config(
			"# a comment \n"
			"1  tmp:       ssd    # temporary files only on SSDs!\n"
			"     # another comment\n"
			"\n"
			"10 fast     :   _ ssd\n"
			"11 safe     :   _ local overseas\n"
			"12 fast_safe:   local ssd overseas\n"
			"19 blahbah: _    _   hdd"
	);
	GoalConfigLoader loader;
	ASSERT_NO_THROW(loader.load(std::istringstream(config)));
	EXPECT_GOAL(loader, 1,  "tmp",       LABELS("ssd"));
	EXPECT_GOAL(loader, 10, "fast",      LABELS(ANY, "ssd"));
	EXPECT_GOAL(loader, 11, "safe",      LABELS(ANY, "local", "overseas"));
	EXPECT_GOAL(loader, 12, "fast_safe", LABELS("local", "ssd", "overseas"));
	EXPECT_GOAL(loader, 19, "blahbah",   LABELS(ANY, ANY, "hdd"));

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

	// duplicates
	EXPECT_THROW(TRY_PARSE("1 1: _\n2 2: _ _\n2: 3 _ _"), ParseException);

	#undef TRY_PARSE
}
