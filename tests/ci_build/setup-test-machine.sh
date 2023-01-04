#!/usr/bin/env bash
set -eux -o pipefail
readonly script_dir="$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")"

die() {  echo "Error: ${*}" >&2;  exit 1; }

# Install necessary packages
apt-get install -y fuse libfuse-dev
# necessary for debian
apt-get install -y sudo valgrind git rsyslog psmisc
apt-get install -y time bc
apt-get install -y vim

# Run LizardFS setup script
mkdir -p /mnt/hdd_0 /mnt/hdd_1 /mnt/hdd_2 /mnt/hdd_3 /mnt/hdd_4
readonly setup_machine_script="${script_dir}/../setup_machine.sh"
[ -f "${setup_machine_script}" ] || die "Script not found: ${setup_machine_script}"
sed "s/apt-get install/apt-get install -y/g" "${setup_machine_script}" \
	| grep -v '^[[:space:]]*mount[[:space:]]*[^[:space:]]*$' \
	| bash -x /dev/stdin setup /mnt/hdd_0 /mnt/hdd_1 /mnt/hdd_2 /mnt/hdd_3 /mnt/hdd_4
chown lizardfstest:lizardfstest /mnt/hdd_0 /mnt/hdd_1 /mnt/hdd_2 /mnt/hdd_3 /mnt/hdd_4

# Extras
apt-get install -y \
  libdb-dev \
  libjudy-dev \
  pylint

GTEST_ROOT="${GTEST_ROOT:-"/usr/local"}"
readonly gtest_temp_build_dir="$(mktemp -d)"
apt-get install -y cmake libgtest-dev
cmake -S /usr/src/googletest -B "${gtest_temp_build_dir}" -DCMAKE_INSTALL_PREFIX="${GTEST_ROOT}"
make -C "${gtest_temp_build_dir}" install
rm -rf "${gtest_temp_build_dir:?}"

cp "${script_dir}/60-ip_port_range.conf" /etc/sysctl.d/

cat >> /etc/sudoers.d/lizardfstest <<-EOT
		ALL ALL = NOPASSWD: /usr/bin/tee -a /etc/hosts
EOT
