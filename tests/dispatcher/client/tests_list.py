import os
from typing import List, Set, Tuple


def get_excluded_tests_two_types(
    excluded_tests: List[str], test_suite: str
) -> Set[str]:
    """Returns a list with excluded tests. Each test is there twice, e.g. both
    'LongSystemTests.testname' and 'testname'."""

    excl_tests_two_types = set()  # both test_suite.testname and just testname
    for t in excluded_tests:
        excl_tests_two_types.add(t)
        if "." in t:
            excl_tests_two_types.add(t.split(".")[1])
        else:
            excl_tests_two_types.add(test_suite + "." + t)
    return excl_tests_two_types


def get_gtest_testlist(
    lizardfs_tests_path: str, test_suite: str, excluded_tests: List[str]
) -> List[str]:
    """Returns a list of (all minus excluded) tests in a given test_suite"""

    tests_list = os.popen(
        lizardfs_tests_path + " --gtest_list_tests --gtest_filter=" + test_suite + "*"
    )

    # Remove unnecessary lines - go just to tests' list
    tests_list.readline()
    tests_list.readline()

    excl_tests_two_types = get_excluded_tests_two_types(excluded_tests, test_suite)
    return [t.strip() for t in tests_list if t.strip() not in excl_tests_two_types]
