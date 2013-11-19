#include <fstream>
#include <gtest/gtest.h>

#ifndef TEST_DATA_PATH
#	error "You have to define TEST_DATA_PATH to compile this file"
#endif

#define TO_STRING_AUX(x) #x
#define TO_STRING(x) TO_STRING_AUX(x)

class BashTestingSuite : public testing::Test {
protected:
	void run_test_case(const std::string& file) {
		ASSERT_EQ(0, system("rm -f /tmp/test_err && touch /tmp/test_err && chmod 0777 /tmp/test_err"));
		std::string runScript = TO_STRING(TEST_DATA_PATH) "/run-test.sh";
		std::string command = "ERROR_FILE=/tmp/test_err " + runScript + " " + file;
		int ret = system(command.c_str());
		if (ret != 0) {
			std::string error;
			std::ifstream results("/tmp/test_err");
			if (!results) {
				error = "Script " + file + " crashed";
			} else {
				error.assign(
						std::istreambuf_iterator<char>(results),
						std::istreambuf_iterator<char>());
				if (!error.empty() && *error.rbegin() == '\n') {
					error.erase(error.size() - 1);
				}
			}
			FAIL() << error;
		}
	}
};

#define add_test_case(suite, name) \
	TEST_F(suite, name) { \
		run_test_case( \
			TO_STRING(TEST_DATA_PATH) "/test_suites/" \
			TO_STRING(suite) "/" TO_STRING(name) ".sh" \
		); \
	}

#include "test_suites.h"
#include "test_cases.h"
