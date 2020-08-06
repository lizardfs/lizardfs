#!/usr/bin/python3

import argparse
import os
import sys
from typing import Dict, List, Set, Tuple


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
    workspace: str, test_suite: str, excluded_tests: List[str]
) -> List[str]:
    """Returns a list of (all minus excluded) tests in a given test_suite"""

    tests_list = os.popen(
        workspace + "/install/lizardfs/bin/lizardfs-tests "
        "--gtest_list_tests --gtest_filter=" + test_suite + "*"
    )

    # Remove unnecessary lines - go just to tests' list
    tests_list.readline()
    tests_list.readline()

    excl_tests_two_types = get_excluded_tests_two_types(excluded_tests, test_suite)
    return [t.strip() for t in tests_list if t.strip() not in excl_tests_two_types]


def get_data_testlist(
    workspace: str, test_suite: str, excluded_tests: List[str]
) -> List[Tuple[str, float]]:
    """Returns a list of (all minus excluded) tests in a given test_suite,
    The list is of tuples (test_name, test_duration).
    Data is fetched from tests/data/'test_suite'.txt file"""

    excl_tests_two_types = get_excluded_tests_two_types(excluded_tests, test_suite)

    tests_data = []
    with open(workspace + "/tests/data/" + test_suite + ".txt", "r") as tests_list_data:
        for data_line in tests_list_data:
            div = data_line.split("=")
            if div[0] not in excl_tests_two_types:
                tests_data.append((div[0], float(div[1])))
    return tests_data


def get_tests_list_with_durations(
    workspace: str, test_suite: str, excluded_tests: List[str]
) -> List[Tuple[str, float]]:
    """Returns list with tuples (testname, duration of test)"""

    gtest_testlist = get_gtest_testlist(workspace, test_suite, excluded_tests)
    data_testlist = get_data_testlist(workspace, test_suite, excluded_tests)

    gtest_tests_set = set(gtest_testlist)
    data_tests_set = {t[0] for t in data_testlist}
    lacking_tests = gtest_tests_set - data_tests_set
    if len(lacking_tests) > 0:
        raise Exception(
            "Incomplete data/'test_suite'.txt file.\nDoesn't contain following tests: "
            + str(lacking_tests)
        )

    return data_testlist


def add_to_partition_dict(
    partition_dict: Dict[Tuple[float, int], List[str]], test_info: Tuple[str, float]
) -> Dict[Tuple[float, int], List[str]]:
    test_name, test_duration = test_info

    min_key = min(partition_dict)
    min_duration, min_index = min_key

    new_value = partition_dict[min_key]
    new_value.append(test_name)
    partition_dict[(min_duration + test_duration, min_index)] = new_value
    del partition_dict[min_key]

    return partition_dict


def partition_algorithm(
    tests_list: List[Tuple[str, float]], nodes_count: int
) -> Dict[Tuple[float, int], List[str]]:
    """Algorithm to make partition. Works in a following way:
    Creates 'nodes_count' dicts (total_sum_of_durations, list_of_those_tests).
    Then iterates over tests with descending duration and just adds a given test
    to the dict with currently smallest total_sum_of_durations.
    Very simple, linear or almost linear, and seems to work surprisingly well."""

    partition_dict: Dict[Tuple[float, int], List[str]] = {}
    for i in range(0, nodes_count):
        partition_dict[(0, i)] = []
    for t in sorted(tests_list, key=lambda tup: (-tup[1], tup[0])):
        partition_dict = add_to_partition_dict(partition_dict, t)
    return partition_dict


def print_tests_to_run(tests_to_run: List[str], test_suite: str) -> None:
    """Prints result in a format acceptable by gtest"""
    s = ":".join(test_suite + "." + test_name for test_name in tests_to_run)
    print(s)


def get_tests_data(
    workspace: str,
    test_suite: str,
    excluded_tests: str,
    nodes_count: int,
    node_number: int,
) -> None:
    """Prints lists of tests to be run on a given node, or an error msg"""
    if node_number > nodes_count:
        print("Node_number > Nodes_count. Nothing to do.", file=sys.stderr)
        return

    excluded_tests_list = excluded_tests.split(":") if excluded_tests else []

    try:
        tests_list_with_durations = get_tests_list_with_durations(
            workspace, test_suite, excluded_tests_list
        )
        partition_dict = partition_algorithm(tests_list_with_durations, nodes_count)

        kth_key = list(partition_dict)[node_number - 1]
        tests_to_run = partition_dict[kth_key]
        print_tests_to_run(tests_to_run, test_suite)
    except Exception as e:
        print(e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    """Script which is used to split (all minus manually_excluded) tests from a given test_suite
    into 'nodes_count' groups, so that tests from each group can be run concurrently.
    Goal for the partition is that tests from each group should run for approximately
    the same duration.
    Approximate duration of each test is stored in and fetched from tests/data/'test_suite'.txt"""

    parser = argparse.ArgumentParser(
        description="Filter tests for concurrent runs. All following arguments besides 'excluded_tests' are required."
    )
    parser.add_argument(
        "-w", "--workspace", type=str, help="Path to lizardfs test directory"
    )
    parser.add_argument("-s", "--test_suite", type=str, help="Name of test_suite")
    parser.add_argument(
        "-e",
        "--excluded_tests",
        type=str,
        help="Names of excluded tests, separated by ':'",
    )
    parser.add_argument(
        "-c", "--nodes_count", type=int, help="Number of nodes on which we run tests"
    )
    parser.add_argument("-n", "--node_number", type=int, help="Number of this node")

    args = parser.parse_args()

    if (
        not args.workspace
        or not args.test_suite
        or not args.nodes_count
        or not args.node_number
    ):
        parser.print_help(sys.stderr)
        sys.exit(1)

    get_tests_data(
        args.workspace,
        args.test_suite,
        args.excluded_tests,
        args.nodes_count,
        args.node_number,
    )
