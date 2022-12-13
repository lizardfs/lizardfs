#!/usr/bin/env bash
set -eux -o pipefail
script_dir="$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")"

die() {  echo "Error: ${*}" >&2;  exit 1; }
extract_paragraphs() {
	local search="${1}"
	local file="${2}"
	awk -v RS='' '/'"${search}"'/' "${file}"
}

setup_machine_script="${script_dir}/../setup_machine.sh"
[ -f "${setup_machine_script}" ] || die "Script not found: ${setup_machine_script}"

extract_paragraphs 'echo Install necessary programs' "${setup_machine_script}" | \
	sed "s/apt-get install/apt-get install -y/g" | \
	bash -x /dev/stdin

# Extras
apt-get install -y \
  libdb-dev \
  libjudy-dev
