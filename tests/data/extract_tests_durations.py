#!/usr/bin/env python3

# Script which takes log files from jenkins' build, and returns a list
# of \n-separated `testname`=`duration of that test on that build`.
#
# ********************************* USAGE *******************************
# ./extract_test_durations.py TestSuite PathToLog [PathToLog ...]
# TestSuite - name of test_suite, required to be one of options defined
#               a few lines below (case insensitive)
# PathToLog - if there are multiple, script will output average durations
#               on those multiple builds.

import os
import re
import sys
from enum import Enum
from typing import Dict, List, TextIO, Tuple


SANITY_NAMES = ["SanityChecks", "Sanity"]
SHORT_NAMES = ["ShortSystemTests", "Shorty", "ShortTests", "Short", "Shorts"]
LONG_NAMES = ["LongSystemTests", "Longi", "Long", "LongTests"]


class TestSuite(Enum):
    SANITY = SANITY_NAMES[0]
    SHORT = SHORT_NAMES[0]
    LONG = LONG_NAMES[0]


def create_test_suite_enum(test_suite_arg: str) -> TestSuite:
    for n in SANITY_NAMES:
        if test_suite_arg.lower() == n.lower():
            return TestSuite.SANITY
    for n in SHORT_NAMES:
        if test_suite_arg.lower() == n.lower():
            return TestSuite.SHORT
    for n in LONG_NAMES:
        if test_suite_arg.lower() == n.lower():
            return TestSuite.LONG

    raise Exception("Wrong test_suite argument. Try sanity/shorts/longs.")


def process_one_build(path_to_log: str, test_suite: TestSuite) -> List[Tuple[str, int]]:
    """If test_suite == Sanity: just processes one given log
    Else: process multiple logs from ConcurrentJobs"""
    tests = []
    with open(path_to_log, "r") as log:
        if test_suite == TestSuite.SANITY:
            tests = parse_logfile_for_tests(log, test_suite)
        else:
            subjobs = get_subjobs_from_mainjob_log(log)
            paths_to_logs = [
                convert_to_correct_subjob_path(sj[0], sj[1], path_to_log)
                for sj in subjobs
            ]
            for p in paths_to_logs:
                try:
                    with open(p, "r") as log:
                        tests.extend(parse_logfile_for_tests(log, test_suite))
                except FileNotFoundError:
                    print(
                        "Error: log for subjob build {} not found (main_job log: {}). Not counting this build at all.".format(
                            p, path_to_log
                        ),
                        file=sys.stderr,
                    )
                    return []
    if len(tests) == 0:
        print("Error: Empty or wrong logfile: {}".format(path_to_log), file=sys.stderr)
    return tests


def parse_logfile_for_tests(
    log_file: TextIO, test_type: TestSuite
) -> List[Tuple[str, int]]:
    result = []
    pattern = test_type.value + "\.(\w+) \((\d+)"
    for line in log_file:
        match = re.search(pattern, line)
        if match:
            test_name = match.group(1)
            test_time = int(match.group(2))
            result.append((test_name, test_time))
    return result


def get_subjobs_from_mainjob_log(log: TextIO) -> List[Tuple[str, str]]:
    JOB_NAME = "lizardfs.ConcurrentJob"
    concurrent_job_re = re.compile(
        "Finished Build.*#(\d+).*{}.*with status".format(JOB_NAME)
    )
    concurrent_jobs = []
    for line in log:
        m = concurrent_job_re.search(line)
        if not m:
            continue
        concurrent_jobs.append((JOB_NAME, m.group(1)))
    return concurrent_jobs


def convert_to_correct_subjob_path(
    job_name: str, build_nr: str, path_to_mainjob_log: str
) -> str:
    return (
        os.path.dirname(path_to_mainjob_log)
        + "/../../../"
        + job_name
        + "/builds/"
        + build_nr
        + "/log"
    )


def print_result(tests: Dict[str, List[int]]) -> None:
    for name in sorted(tests):
        durations = tests[name]
        avg_duration = sum(durations) / len(durations)
        print("{}={}".format(name, int(avg_duration)))


def print_help_message() -> None:
    print(
        "*********************** USAGE ***********************\n"
        + "./extract_test_durations.py TestSuite PathToLog [PathToLog ...],\n"
        + "e.g. ./extract_test_durations.py sanity 123456/log 123457/log",
        file=sys.stderr,
    )


def main() -> None:
    if len(sys.argv) < 3:
        print("Wrong number of arguments.", file=sys.stderr)
        print_help_message()
        return

    test_suite_name = sys.argv[1]
    test_suite = create_test_suite_enum(test_suite_name)
    paths_to_logs = sys.argv[2:]

    tests = {}
    for path in paths_to_logs:
        cur_tests = process_one_build(path, test_suite)
        for name, duration in cur_tests:
            if name not in tests:
                tests[name] = [duration]
            else:
                tests[name].append(duration)
    print_result(tests)


if __name__ == "__main__":
    main()
