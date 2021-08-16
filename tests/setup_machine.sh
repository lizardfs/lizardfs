#!/usr/bin/env bash

usage() {
	cat >&2 <<EOF
Usage: $0 setup hdd...

This scripts prepares the machine to run LizardFS tests here.
Specifically:
* creates users lizardfstest, lizardfstest_0, ..., lizardfstest_9
* adds all lizardfstest users to the fuse group
* grants all users rights to run programs as lizardfstest users
* grants all users rights to run 'pkill -9 -u <some lizardfstest user>'
* allows all users to mount fuse filesystem with allow_other option
* creates a 2G ramdisk in /mnt/ramdisk
* creates 6 files mounted using loop device

Example:
$0 setup /mnt
$0 setup /mnt/hda /mnt/hdb

You need root permissions to run this script
EOF
	exit 1
}

minimum_number_of_args=2
if [[ ( ( "${1}" != "setup" ) && ( "${1}" != "setup-force" ) ) || ( ${#} -lt ${minimum_number_of_args} ) ]]; then
	usage >&2
fi

grep -q lizardfstest_loop /etc/fstab
if [[ ( "${1}" != "setup-force" ) && ( ${?} == 0 ) ]]; then
	echo The machine is at least partialy configured
	echo Run revert-setup-machine.sh to revert the current configuration
	exit 1
fi

shift
umask 0022

# Make this script safe and bug-free ;)
set -eux

echo ; echo Install necessary programs
# lsb_release is required by both build scripts and this script -- install it first
if ! command -v lsb_release; then
	if command -v dnf; then
		dnf -y install redhat-lsb-core
	elif command -v yum; then
		yum -y install redhat-lsb-core
	elif command -v apt-get; then
		apt-get -y install lsb-release
	fi
fi
# determine which OS we are running and choose the right set of packages to be installed
release="$(lsb_release -si)/$(lsb_release -sr)"
case "$release" in
	LinuxMint/*|Ubuntu/*|Debian/*)
		apt-get -y install asciidoc build-essential cmake debhelper devscripts git fuse3 libfuse3-dev
		apt-get -y install libfuse-dev pkg-config zlib1g-dev libboost-program-options-dev
		apt-get -y install libboost-system-dev acl attr dbench netcat-openbsd pylint python3 rsync
		apt-get -y install socat tidy wget libgoogle-perftools-dev libboost-filesystem-dev
		apt-get -y install libboost-iostreams-dev libpam0g-dev libdb-dev nfs4-acl-tools libfmt-dev
		apt-get -y install python3-pip valgrind ccache libfmt-dev libisal-dev libcrcutil-dev curl
		apt-get -y install libgtest-dev libspdlog-dev libjudy-dev time bc
		pip3 install mypy black flask requests types-requests
		;;
	CentOS/7*)
		yum -y install asciidoc cmake fuse-devel git gcc gcc-c++ make pkgconfig rpm-build zlib-devel
		yum -y install acl attr dbench nc pylint rsync socat tidy wget gperftools-libs
		yum -y install boost-program-options boost-system libboost-filesystem libboost-iostreams
		yum -y install pam-devel libdb-devel nfs4-acl-tools time bc
		;;
	CentOS/8*)
		dnf -y install asciidoc cmake fuse-devel git gcc gcc-c++ make pkgconfig rpm-build zlib-devel
		dnf -y install acl attr dbench nc pylint rsync socat tidy wget gperftools-libs
		dnf -y install boost-program-options boost-system boost-filesystem boost-iostreams
		dnf -y install pam-devel libdb-devel nfs4-acl-tools fuse3 fuse3-devel
		dnf -y install fmt-devel spdlog-devel boost-devel libtirpc-devel time bc
		dnf -y install --enablerepo=PowerTools gtest-devel
		# install openbsd version of netcat
		dnf -y install epel-release
		dnf -y update
		dnf -y install --enablerepo=epel-testing netcat
		update-alternatives --install /usr/bin/nc nc /usr/bin/netcat 1
		pip3 install black mypy
		;;
	Fedora/*)
		dnf -y install cmake gcc-c++ gtest-devel fmt-devel spdlog-devel fuse-devel fuse3-devel boost-devel
		dnf -y install Judy-devel pam-devel libdb-devel thrift-devel valgrind pylint nfs4-acl-tools
		dnf -y install libtirpc-devel time dbench bc
		# install openbsd version of netcat
		dnf -y install netcat
		update-alternatives --install /usr/bin/nc nc /usr/bin/netcat 1
		pip3 install black mypy
		;;
	*)
		set +x
		echo "Installation of required packages SKIPPED, '$release' isn't supported by this script"
		;;
esac

echo ; echo Add group fuse
groupadd -f fuse

echo ; echo Add user lizardfstest
if ! getent passwd lizardfstest > /dev/null; then
	useradd --system --user-group --home /var/lib/lizardfstest lizardfstest
fi
if ! groups lizardfstest | grep -w fuse > /dev/null; then
	usermod -a -G fuse lizardfstest # allow this user to mount fuse filesystem
fi
if ! groups lizardfstest | grep -w adm > /dev/null; then
	usermod -a -G adm lizardfstest # allow this user to read files from /var/log
fi

echo ; echo Create home directory /var/lib/lizardfstest
if [[ ! -d /var/lib/lizardfstest ]]; then
	mkdir -p /var/lib/lizardfstest
	chown lizardfstest:lizardfstest /var/lib/lizardfstest
	chmod 755 /var/lib/lizardfstest
fi

echo ; echo Prepare sudo configuration
if ! [[ -d /etc/sudoers.d ]]; then
	# Sudo is not installed by default on Debian machines
	echo "sudo not installed!" >&2
	echo "Install it manually: apt-get install sudo" >&2
	echo "Then run this script again" >&2
	exit 1
fi
if ! [[ -f /etc/sudoers.d/lizardfstest ]] || \
		! grep drop_caches /etc/sudoers.d/lizardfstest >/dev/null; then
	cat >/etc/sudoers.d/lizardfstest <<-END
		ALL ALL = (lizardfstest) NOPASSWD: ALL
		ALL ALL = NOPASSWD: /usr/bin/pkill -9 -u lizardfstest
		ALL ALL = NOPASSWD: /bin/rm -rf /tmp/lizardfs_error_dir
		lizardfstest ALL = NOPASSWD: /bin/sh -c echo\ 1\ >\ /proc/sys/vm/drop_caches
	END
	chmod 0440 /etc/sudoers.d/lizardfstest
fi
if ! [[ -d /etc/security/limits.d ]]; then
	echo "pam module pam_limits is not installed!" >&2
	exit 1
fi
if ! [[ -f /etc/security/limits.d/10-lizardfstests.conf ]]; then
	# Change limit of open files for user lizardfstest
	echo "lizardfstest hard nofile 10000" > /etc/security/limits.d/10-lizardfstests.conf
	chmod 0644 /etc/security/limits.d/10-lizardfstests.conf
fi
if ! grep 'pam_limits.so' /etc/pam.d/sudo > /dev/null; then
	cat >>/etc/pam.d/sudo <<-END
		### Reload pam limits on sudo - necessary for lizardfs tests. ###
		session required pam_limits.so
	END
fi

echo ; echo 'Add users lizardfstest_{0..9}'
for username in lizardfstest_{0..9}; do
	if ! getent passwd $username > /dev/null; then
		useradd --system --user-group --home /var/lib/$username --create-home \
				--groups fuse,lizardfstest $username
		cat >>/etc/sudoers.d/lizardfstest <<-END

			ALL ALL = ($username) NOPASSWD: ALL
			ALL ALL = NOPASSWD: /usr/bin/pkill -9 -u $username
		END
	fi
done

echo ; echo 'Fixing GIDs of users'
for name in lizardfstest lizardfstest_{0..9}; do
	uid=$(getent passwd "$name" | cut -d: -f3)
	gid=$(getent group  "$name" | cut -d: -f3)
	if [[ $uid == "" || $gid == "" ]]; then
		exit 1
	fi
	if [[ $uid == $gid ]]; then
		# UID is equal to GID -- we have to change it to something different,
		# so find some other free gid
		newgid=$((gid * 2))
		while getent group $newgid; do
			newgid=$((newgid + 1))
		done
		groupmod -g $newgid $name
	fi
done

echo ; echo Prepare /etc/fuse.conf
if ! grep '^[[:blank:]]*user_allow_other' /etc/fuse.conf >/dev/null; then
	echo "user_allow_other" >> /etc/fuse.conf
fi

echo ; echo Prepare empty /etc/lizardfs_tests.conf
if ! [[ -f /etc/lizardfs_tests.conf ]]; then
	cat >/etc/lizardfs_tests.conf <<-END
		: \${LIZARDFS_DISKS:="$*"}
		# : \${TEMP_DIR:=/tmp/LizardFS-autotests}
		# : \${LIZARDFS_ROOT:=$HOME/local}
		# : \${FIRST_PORT_TO_USE:=25000}
	END
fi

echo ; echo Prepare ramdisk
if ! grep /mnt/ramdisk /etc/fstab >/dev/null; then
	echo "# Ramdisk used in LizardFS tests" >> /etc/fstab
	echo "ramdisk  /mnt/ramdisk  tmpfs  mode=1777,size=2g" >> /etc/fstab
	mkdir -p /mnt/ramdisk
	mount /mnt/ramdisk
	echo ': ${RAMDISK_DIR:=/mnt/ramdisk}' >> /etc/lizardfs_tests.conf
fi

echo ; echo Prepare loop devices
#creating loop devices more or less evenly distributed between available disks
i=0
devices=6
loops=()
while [ $i -lt $devices ] ; do
	for disk in "$@"; do
		if (( i == devices )); then #stop if we have enough devices
			break
		fi
		loops+=(/mnt/lizardfstest_loop_$i)
		if grep -q lizardfstest_loop_$i /etc/fstab; then
			(( ++i ))
			continue
		fi
		mkdir -p "$disk/lizardfstest_images"
		# Create image file
		image="$disk/lizardfstest_images/image_$i"
		truncate -s 1G "$image"
		mkfs.ext4 -Fq "$image"
		# Add it to fstab
		echo "$(readlink -m "$image") /mnt/lizardfstest_loop_$i  ext4  loop" >> /etc/fstab
		mkdir -p /mnt/lizardfstest_loop_$i
		# Mount and set permissions
		mount /mnt/lizardfstest_loop_$i
		chmod 1777 /mnt/lizardfstest_loop_$i
		(( ++i ))
	done
done
echo ': ${LIZARDFS_LOOP_DISKS:="'"${loops[*]}"'"}' >> /etc/lizardfs_tests.conf

set +x
echo Machine configured successfully
