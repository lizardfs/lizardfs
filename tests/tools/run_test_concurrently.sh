#!/usr/bin/env bash

# Script which is currently responsible for running tests on Jenkins.
# It can also be run manually.
#
# It can either run all tests from a given test_suite on this machine
# (no parallelisation, like it used to be).
# Or only run a part of tests (other parts should be run on another
# machines.) If e.g. NODES_COUNT=3 and you run this script on 3 machines,
# one with NODE_NUMBER=1, one with NODE_NUMBER=2, and one with NODE_NUMBER=3,
# then tests will be split into 3 parts, and each test will be in exactly
# one part, run on only one machine.
# Thanks to that we achieve faster tests' runtimes.
#
# USAGE:
# ./run_test_concurrently.sh list_of_arguments
# All arguments should be passed in a following way: ARG_NAME=value
# List of arguments:
# WORKSPACE     - path to lizardfs directory
# TEST_SUITE    - name of test_suite, e.g. SanityChecks
# EXCLUDE_TESTS - list of tests we don't want to run, separated by ':',
#                 e.g. test_valgrind:test_dirinfo
# RUN_UNITTEST  - 'true' if we want to run them.
# NODES_COUNT   - number of machines on which we run tests concurrently
# NODE_NUMBER   - number of current machine (integer from [1..NODES_COUNT])
# VALGRIND      - "Yes" <=> we want to run all tests under valgrind

set -o errexit -o nounset -o errtrace -o pipefail

for ARGUMENT in "$@"
do
	KEY=$(echo $ARGUMENT | cut -f1 -d=)
	VALUE=$(echo $ARGUMENT | cut -f2 -d=)

	case "$KEY" in
		WORKSPACE)      WORKSPACE=${VALUE} ;;
		TEST_SUITE)     TEST_SUITE=${VALUE} ;;
		EXCLUDE_TESTS)  EXCLUDE_TESTS=${VALUE} ;;
		RUN_UNITTESTS)  RUN_UNITTESTS=${VALUE} ;;
		NODES_COUNT)    NODES_COUNT=${VALUE} ;;
		NODE_NUMBER)    NODE_NUMBER=${VALUE} ;;
		VALGRIND)       VALGRIND=${VALUE} ;;
		*)
	esac
done

WORKSPACE=${WORKSPACE:-$(dirname $0)/../..}
EXCLUDE_TESTS=${EXCLUDE_TESTS:-'""'}
NODES_COUNT=${NODES_COUNT:-1}
NODE_NUMBER=${NODE_NUMBER:-1}
RUN_UNITTESTS=${RUN_UNITTESTS:-'false'}
VALGRIND=${VALGRIND:-'No'}

export LIZARDFS_ROOT=$WORKSPACE/install/lizardfs
export TEST_OUTPUT_DIR=$WORKSPACE/test_output
export TERM=xterm

if [ $VALGRIND == "Yes" ]; then
	export USE_VALGRIND=YES;
fi

# TODO: maybe somehow implement the following lines, to not run compilation,
# when it's unnecessary.
# Those don't work well - jenkins will treat such a test as an error,
# and it looks ugly in build results. Maybe create a mock log file?
# if [ $NODE_NUMBER -gt $NODES_COUNT ]; then
# exit 0
# fi

make -C build/lizardfs -j$(nproc) install

killall -9 lizardfs-tests || true
mkdir -m 777 -p $TEST_OUTPUT_DIR
rm -rf $TEST_OUTPUT_DIR/* || true
rm -rf /mnt/ramdisk/* || true

FILTER=$(python3 $WORKSPACE/tests/tools/filter_tests.py --workspace $WORKSPACE --test_suite $TEST_SUITE \
	--excluded_tests $EXCLUDE_TESTS --nodes_count $NODES_COUNT --node_number $NODE_NUMBER)

if [ $RUN_UNITTESTS == "true" ]; then
	$WORKSPACE/build/lizardfs/src/unittests/unittests --gtest_color=yes --gtest_output=xml:$TEST_OUTPUT_DIR/unit_test_results.xml
fi

$LIZARDFS_ROOT/bin/lizardfs-tests --gtest_color=yes --gtest_filter=$FILTER --gtest_output=xml:$TEST_OUTPUT_DIR/test_results.xml
