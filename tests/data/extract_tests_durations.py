#!/usr/bin/env python3

import re
import sys
from typing import List, TextIO, Tuple


def parse_logfile_for_tests(log_file: TextIO, test_type: str) -> List[Tuple[str, int]]:
    result = []
    pattern = test_type + "\.(\w+) \((\d+)"
    for line in log_file:
        match = re.search(pattern, line)
        if match:
            test_name = match.group(1)
            test_time = int(match.group(2))
            result.append((test_name, test_time))
    return result


def print_result(tests: List[Tuple[str, int]]) -> None:
    for name, time in sorted(tests):
        print(name + "=" + str(time))


def print_help_message() -> None:
    print(
        "*********************** USAGE ***********************\n"
        + "./extract_test_durations.py log_filename test_suite,\n"
        + "e.g. ./extract_test_durations.py log SanityChecks",
        file=sys.stderr,
    )


def main() -> None:
    """Script which takes a log file from jenkins' build, and returns a list
    of \n-separated `testname`=`duration of that test on that build`"""
    if len(sys.argv) != 3:
        print("Wrong number of arguments.", file=sys.stderr)
        print_help_message()
        return

    path_to_log = sys.argv[1]
    test_type = sys.argv[2]

    with open(path_to_log, "r") as f:
        tests = parse_logfile_for_tests(f, test_type)
        if len(tests) == 0:
            print("Error: Empty or wrong logfile.", file=sys.stderr)
        else:
            print_result(tests)


if __name__ == "__main__":
    main()
