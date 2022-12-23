#!/usr/bin/env bash
readonly script_dir="$(readlink -f "$(dirname "${SCRIPT_SOURCE[0]}")")"
set -eux -o pipefail

get_build_date() {
	date -u +"%Y%m%d-%H%M%S"
}

get_commit_id() {
	git rev-parse --short HEAD
}

get_cvs_branch() {
	local commit_id=${1:-HEAD}
	basename "$(git branch -r --contains "${commit_id}" | head -n1)"
}

get_last_header() {
	grep -Eie '(^lizardfs|mfs).*urgency' debian/changelog | head -n1
}
export -f get_last_header

get_app_version() {
	 get_last_header | awk '{print $2}' | tr -d '()'
}

get_version_metadata_string() {
	local branch_name branch_status commit_id timestamp version
	commit_id="$(get_commit_id)"
	branch_name="$(get_cvs_branch "${commit_id}")"
	branch_status="unstable"
	if [ "${branch_name}" == "main" ]; then
		branch_status="stable"
	fi
	timestamp="$(get_build_date)"
	version="$(get_app_version)"
	echo "${version}-${timestamp}-${branch_status}-${branch_name}-${commit_id}"
}

WORKSPACE="${WORKSPACE:-"${script_dir}"}"

OSNAME=$(lsb_release -si)
RELEASE=$(lsb_release -sr | sed 's@n/a@testing@')
VERSION_LONG_STRING="$(get_version_metadata_string)"
BUNDLE="lizardfs-bundle-${OSNAME}-${RELEASE}-${VERSION_LONG_STRING}"

MAKEFLAGS="-j$(nproc)"
export MAKEFLAGS VERSION_LONG_STRING WORKSPACE

rm -rf lizardfs-bundle-*
mkdir "${BUNDLE}"
cd "${BUNDLE}"

case "${OSNAME}" in
	Ubuntu|Debian)
		"${WORKSPACE}/create-deb-package.sh"
		;;
	CentOS|Fedora)
		"${WORKSPACE}/create-rpm-package.sh"
		;;
	*)
		echo "Unsupported Operating system"
		exit 1
		;;
esac

cd ${WORKSPACE}
tar -cf ${BUNDLE}.tar ${BUNDLE}
