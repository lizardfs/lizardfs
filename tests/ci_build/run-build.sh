#!/usr/bin/env bash
set -eux -o pipefail
PROJECT_DIR="$(readlink -f "$(dirname "${BASH_SOURCE[0]}")/../..")"
WORKSPACE=${WORKSPACE:-"${PROJECT_DIR}"}
die() { echo "Error: $*" >&2; exit 1; }

usage() {
	cat <<-EOT
	Builds lizardfs with different configurations

	Usage: run-build.sh [OPTION]

	Options:
	  coverage   Build with parameters for coverage report
	  test       Build for test
	  release    Build with no debug info
	EOT
	exit 1
}

declare -a CMAKE_LIZARDFS_ARGUMENTS=(
  -G 'Unix Makefiles'
	-DENABLE_DOCS=ON
	-DENABLE_CLIENT_LIB=ON
	-DENABLE_URAFT=ON
	-DENABLE_NFS_GANESHA=OFF
	-DENABLE_POLONAISE=OFF
)

[ -n "${1:-}" ] || usage
build_type="${1}"
shift
case "${build_type,,}" in
	coverage)
		CMAKE_LIZARDFS_ARGUMENTS+=(
			-DCMAKE_BUILD_TYPE=Debug
			-DCMAKE_INSTALL_PREFIX="${WORKSPACE}/install/lizardfs/"
			-DENABLE_TESTS=ON
			-DCODE_COVERAGE=ON
			-DLIZARDFS_TEST_POINTER_OBFUSCATION=ON
			-DENABLE_WERROR=OFF
		)
		;;
	test)
		CMAKE_LIZARDFS_ARGUMENTS+=(
			-DCMAKE_BUILD_TYPE=RelWithDbInfo
			-DCMAKE_INSTALL_PREFIX="${WORKSPACE}/install/lizardfs/"
			-DENABLE_TESTS=ON
			-DCODE_COVERAGE=OFF
			-DLIZARDFS_TEST_POINTER_OBFUSCATION=ON
			-DENABLE_WERROR=ON
		)
		;;
	release)
		CMAKE_LIZARDFS_ARGUMENTS+=(
			-DCMAKE_BUILD_TYPE=Release
			-DCMAKE_INSTALL_PREFIX=/
			-DENABLE_TESTS=OFF
			-DCODE_COVERAGE=OFF
			-DLIZARDFS_TEST_POINTER_OBFUSCATION=OFF
			-DENABLE_WERROR=OFF
		)
		;;
	*) die "Unsupported build type: ${build_type}"
		;;
esac

if [ -n "${PACKAGE_VERSION:-}" ]; then
	CMAKE_LIZARDFS_ARGUMENTS+=( -DPACKAGE_VERSION="${PACKAGE_VERSION}" )
fi

declare -a EXTRA_ARGUMENTS=("${@}")
rm -rf "${WORKSPACE:?}/build/lizardfs"/{,.}* 2>/dev/null || true
cmake -B "${WORKSPACE}/build/lizardfs" \
	"${CMAKE_LIZARDFS_ARGUMENTS[@]}" \
	"${EXTRA_ARGUMENTS[@]}" "${WORKSPACE}"

JOBS=${JOBS:-$(($(nproc) * 3 / 4 + 1))}
make -C "${WORKSPACE}/build/lizardfs" -j "${JOBS}" install
