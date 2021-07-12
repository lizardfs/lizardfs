# Legacy version of LizardFS used in tests and sources of its packages
LIZARDFSXX_TAG="3.12.0"

install_lizardfsXX() {
	rm -rf "$LIZARDFSXX_DIR"
	mkdir -p "$LIZARDFSXX_DIR"
	local distro="$(lsb_release -si)"
	case "$distro" in
		Ubuntu|Debian)
			local codename="$(lsb_release -sc)"
			mkdir -p ${TEMP_DIR}/apt/var/lib/apt/partial
			mkdir -p ${TEMP_DIR}/apt/var/cache/apt/archives/partial
			mkdir -p ${TEMP_DIR}/apt/var/lib/dpkg
			cp /var/lib/dpkg/status ${TEMP_DIR}/apt/var/lib/dpkg/status
			cat >${TEMP_DIR}/apt/apt.conf << END
Dir::State "${TEMP_DIR}/apt/var/lib/apt";
Dir::State::status "${TEMP_DIR}/apt/var/lib/dpkg/status";
Dir::Etc::SourceList "${TEMP_DIR}/apt/lizardfs.list";
Dir::Cache "${TEMP_DIR}/apt/var/cache/apt";
END
			local destdir="${TEMP_DIR}/apt/var/cache/apt/archives"
			echo "deb [trusted=yes] https://dev.lizardfs.com/packages/ ${codename}/" >${TEMP_DIR}/apt/lizardfs.list
			apt-get --config-file ${TEMP_DIR}/apt/apt.conf update
			apt-get -y --allow-downgrades --config-file=${TEMP_DIR}/apt/apt.conf install -d \
				lizardfs-master=${LIZARDFSXX_TAG} \
				lizardfs-chunkserver=${LIZARDFSXX_TAG} \
				lizardfs-client=${LIZARDFSXX_TAG}
			# unpack binaries
			cd ${destdir}
			find . -name "*master*.deb"      | xargs dpkg-deb --fsys-tarfile | tar -x ./usr/sbin/mfsmaster
			find . -name "*chunkserver*.deb" | xargs dpkg-deb --fsys-tarfile | tar -x ./usr/sbin/mfschunkserver
			find . -name "*client*.deb"      | xargs dpkg-deb --fsys-tarfile | tar -x ./usr/bin/
			cp -Rp usr/ ${LIZARDFSXX_DIR_BASE}/install
			cd -
			;;
		CentOS|Fedora)
			local destdir="${TEMP_DIR}/lizardfsxx_packages"
			mkdir ${destdir}
			local url=""
			if [ "$distro" == CentOS ]; then
				url="http://dev.lizardfs.com/packages/centos.lizardfs.repo"
			else
				url="http://dev.lizardfs.com/packages/fedora.lizardfs.repo"
			fi
			mkdir -p ${TEMP_DIR}/dnf/etc/yum.repos.d
			cat >${TEMP_DIR}/dnf/dnf.conf << END
[main]
logdir=${TEMP_DIR}/dnf/var/log
cachedir=${TEMP_DIR}/dnf/var/cache
persistdir=${TEMP_DIR}/dnf/var/lib/dnf
reposdir=${TEMP_DIR}/dnf/etc/yum.repos.d
END
			wget "$url" -O ${TEMP_DIR}/dnf/etc/yum.repos.d/lizardfs.repo
			fakeroot dnf -y --config=${TEMP_DIR}/dnf/dnf.conf --downloadonly --destdir=${destdir} install \
				lizardfs-master-${LIZARDFSXX_TAG} \
				lizardfs-chunkserver-${LIZARDFSXX_TAG} \
				lizardfs-client-${LIZARDFSXX_TAG}
			# unpack binaries
			cd ${destdir}
			find . -name "*master*.rpm"      | xargs rpm2cpio | cpio -idm ./usr/sbin/mfsmaster
			find . -name "*chunkserver*.rpm" | xargs rpm2cpio | cpio -idm ./usr/sbin/mfschunkserver
			find . -name "*client*.rpm"      | xargs rpm2cpio | cpio -idm ./usr/bin/*
			cp -Rp usr/ ${LIZARDFSXX_DIR_BASE}/install
			cd -
			;;
		*)
			test_fail "Your distribution ($distro) is not supported."
			;;
	esac
	test_lizardfsXX_executables
	echo "Legacy LizardFS packages installed."
}

test_lizardfsXX_executables() {
	local awk_version_pattern="/$(sed 's/[.]/[.]/g' <<< $LIZARDFSXX_TAG)/"
	test -x "$LIZARDFSXX_DIR/bin/mfsmount"
	test -x "$LIZARDFSXX_DIR/sbin/mfschunkserver"
	test -x "$LIZARDFSXX_DIR/sbin/mfsmaster"
	assert_awk_finds $awk_version_pattern "$($LIZARDFSXX_DIR/bin/mfsmount --version 2>&1)"
	assert_awk_finds $awk_version_pattern "$($LIZARDFSXX_DIR/sbin/mfschunkserver -v)"
	assert_awk_finds $awk_version_pattern "$($LIZARDFSXX_DIR/sbin/mfsmaster -v)"
}

lizardfsXX_chunkserver_daemon() {
	"$LIZARDFSXX_DIR/sbin/mfschunkserver" -c "${lizardfs_info_[chunkserver$1_cfg]}" "$2" | cat
	return ${PIPESTATUS[0]}
}

lizardfsXX_master_daemon() {
	"$LIZARDFSXX_DIR/sbin/mfsmaster" -c "${lizardfs_info_[master_cfg]}" "$1" | cat
	return ${PIPESTATUS[0]}
}

# A generic function to run LizardFS commands.
#
# Usage examples:
#   mfs mfssetgoal 3 file
#   mfs mfsdirinfo file
#   mfs mfsmetalogger stop
lizardfsXX() {
	local command="$1"
	shift
	"$LIZARDFSXX_DIR/"*bin"/$command" "$@" | cat
	return ${PIPESTATUS[0]}
}

assert_lizardfsXX_services_count_equals() {
	local mas_expected="${1}"
	local cs_expected="${2}"
	local cli_expected="${3}"
	assert_equals "${mas_expected}" "$(lizardfs_admin_master info | grep "${LIZARDFSXX_TAG}" | wc -l)"
	assert_equals "${cs_expected}" "$(lizardfs_admin_master list-chunkservers | grep "${LIZARDFSXX_TAG}" | wc -l)"
	assert_equals "${cli_expected}" "$(lizardfs_admin_master list-mounts | grep "${LIZARDFSXX_TAG}" | wc -l)"
}

assert_no_lizardfsXX_services_active() {
	assert_lizardfsXX_services_count_equals 0 0 0
}
