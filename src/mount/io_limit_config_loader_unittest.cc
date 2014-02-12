#include "mount/io_limit_config_loader.h"

#include <sstream>
#include <gtest/gtest.h>

#define PAIR(a, b) (std::make_pair(a, b))

#define LIMITS(...) (IoLimitConfigLoader::LimitsMap({__VA_ARGS__}))
#define ASSERT_LIMITS_EQ(loader, ...) ASSERT_EQ(LIMITS(__VA_ARGS__), loader.limits())

TEST(IoLimitConfigLoaderTest, CorrectFile) {
	std::string config(
			"subsystem lubie_placuszki\n"
			"limit a 1\n"
			"limit b 2\n"
			"limit c 3\n"
			);
	std::istringstream ss(config);
	IoLimitConfigLoader loader;
	ASSERT_NO_THROW(loader.load(std::move(ss)));
	ASSERT_EQ("lubie_placuszki", loader.subsystem());
	ASSERT_LIMITS_EQ(loader, PAIR("a", 1), PAIR("b", 2), PAIR("c", 3));
}

TEST(IoLimitConfigLoaderTest, SubsystemNotSpecified) {
	std::string config(
			"limit a 1\n"
			"limit b 2\n"
			"limit c 3\n"
			);
	std::istringstream ss(config);
	IoLimitConfigLoader loader;
	ASSERT_THROW(loader.load(std::move(ss)), IoLimitConfigLoader::ParseException);
}

TEST(IoLimitConfigLoaderTest, IncorrectLimit) {
	std::string config(
			"subsystem trololo\n"
			"limit a 1\n"
			"limit b cookie_monster\n"
			"limit c 3\n"
			);
	std::istringstream ss(config);
	IoLimitConfigLoader loader;
	ASSERT_THROW(loader.load(std::move(ss)), IoLimitConfigLoader::ParseException);
}

TEST(IoLimitConfigLoaderTest, UnknownKeyword) {
	std::string config(
			"subsystem trololo\n"
			"limit a 1\n"
			"limit b 45\n"
			"Agnieszka"
			"limit c 3\n"
			);
	std::istringstream ss(config);
	IoLimitConfigLoader loader;
	ASSERT_THROW(loader.load(std::move(ss)), IoLimitConfigLoader::ParseException);
}

TEST(IoLimitConfigLoaderTest, RepeatedGroup) {
	std::string config(
			"subsystem trololo\n"
			"limit a 1\n"
			"limit b 45\n"
			"limit a 3\n"
			);
	std::istringstream ss(config);
	IoLimitConfigLoader loader;
	ASSERT_THROW(loader.load(std::move(ss)), IoLimitConfigLoader::ParseException);
}

TEST(IoLimitConfigLoaderTest, Comment) {
	std::string config(
			"#\n"
			"subsystem trololo\n"
			"  #  limit a 1\n"
			"limit b 45\n"
			"#limit a 3\n"
			"  limit c 1#RANDOM_TEXT  \n"
			);
	std::istringstream ss(config);
	IoLimitConfigLoader loader;
	ASSERT_NO_THROW(loader.load(std::move(ss)));
	ASSERT_EQ("trololo", loader.subsystem());
	ASSERT_LIMITS_EQ(loader, PAIR("b", 45), PAIR("c", 1));
}
