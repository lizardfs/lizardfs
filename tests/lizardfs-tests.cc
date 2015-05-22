#include <fstream>
#include <gtest/gtest.h>

#ifndef TEST_DATA_PATH
#       error "You have to define TEST_DATA_PATH to compile this file"
#endif

#define TO_STRING_AUX(x) #x
#define TO_STRING(x) TO_STRING_AUX(x)

class BashTestingSuite : public testing::Test {
protected:
	void run_test_case(std::string suite, std::string testCase) {
		ASSERT_EQ(0, system("rm -f /tmp/test_err && touch /tmp/test_err && chmod 0777 /tmp/test_err"));
		std::string runScript = TO_STRING(TEST_DATA_PATH) "/run-test.sh";
		std::string testFile = TO_STRING(TEST_DATA_PATH) "/test_suites/" + suite + "/" + testCase + ".sh";
		std::string environment = "ERROR_FILE=/tmp/test_err";
		environment += " TEST_SUITE_NAME=" + suite;
		environment += " TEST_CASE_NAME=" + testCase;
		std::string command = environment + " " + runScript + " " + testFile;
		int ret = system(command.c_str());
		if (ret != 0) {
			std::string error;
			std::ifstream results("/tmp/test_err");
			if (!results) {
				error = "Script " + testFile + " crashed";
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

#define add_test_case(suite, testCase) \
		TEST_F(suite, testCase) { run_test_case(TO_STRING(suite), TO_STRING(testCase)); }

#include "test_suites.h"
#include "test_cases.h"
