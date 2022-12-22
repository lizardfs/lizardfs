#!/usr/bin/env bash
set -eux -o pipefail
PROJECT_DIR="$(readlink -f "$(dirname "${BASH_SOURCE[0]}")/../..")"
WORKSPACE=${WORKSPACE:-"${PROJECT_DIR}"}
die() { echo "Error: $*" >&2; exit 1; }

export LIZARDFS_ROOT=${WORKSPACE}/install/lizardfs
echo "LIZARDFS_ROOT: ${LIZARDFS_ROOT}"
export TEST_OUTPUT_DIR=${WORKSPACE}/test_output
echo "TEST_OUTPUT_DIR: ${TEST_OUTPUT_DIR}"
export TERM=xterm

killall -9 lizardfs-tests || true
mkdir -m 777 -p "${TEST_OUTPUT_DIR}"
rm -rf "${TEST_OUTPUT_DIR:?}"/* || true
rm -rf /mnt/ramdisk/* || true
[ -f "${LIZARDFS_ROOT}/bin/lizardfs-tests" ] || \
	die "${LIZARDFS_ROOT}/bin/lizardfs-tests" not found, did you build the project?
export PATH="${LIZARDFS_ROOT}/bin:${PATH}"
"${LIZARDFS_ROOT}/bin/lizardfs-tests" --gtest_color=yes --gtest_filter='SanityChecks.*' --gtest_output=xml:"${TEST_OUTPUT_DIR}/sanity_test_results.xml"
