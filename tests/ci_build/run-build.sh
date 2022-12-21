#!/usr/bin/env bash
set -eux -o pipefail
PROJECT_DIR="$(readlink -f "$(dirname "${BASH_SOURCE[0]}")/../..")"
WORKSPACE=${WORKSPACE:-"${PROJECT_DIR}"}
die() { echo "Error: $*" >&2; exit 1; }

rm -rf "${WORKSPACE:?}/build/lizardfs"/{,.}* || true

EXTRA_ARGUMENTS="${*}"
cmake -B "${WORKSPACE}/build/lizardfs" \
	-G 'Unix Makefiles' \
	-DCMAKE_BUILD_TYPE=RelWithDbInfo \
	-DENABLE_TESTS=YES \
	-DCMAKE_INSTALL_PREFIX="${WORKSPACE}/install/lizardfs/" \
	-DENABLE_WERROR=NO \
	-DLIZARDFS_TEST_POINTER_OBFUSCATION=1 \
	-DENABLE_CLIENT_LIB=YES \
	-DENABLE_NFS_GANESHA=NO \
	-DENABLE_POLONAISE=NO \
	${EXTRA_ARGUMENTS} "${WORKSPACE}"

JOBS=${JOBS:-$(($(nproc) * 3 / 4 + 1))}
make -C "${WORKSPACE}/build/lizardfs" -j "${JOBS}" install
