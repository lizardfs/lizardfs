#!/bin/bash

usage() {
	echo "Usage: $0 setup"
	echo
	echo "This scripts prepares the machine to run LizardFS tests here."
	echo "Specifically:"
	echo "* creates user lizardfstest"
	echo "* adds user lizardfstestto the fuse group"
	echo "* grants all users rights to run programs as lizardfstest"
	echo "* grants all users rights to run 'pkill -9 -u lizardfstest"
	echo "* allow all users to mount fuse filesystem with allow_other option"
	echo
	echo "You need root permissions to run this script"
	exit 1
}

if [[ $1 != setup ]]; then
	usage >&2
fi

# Make this script safe and bug-free ;)
set -eux

echo Looking for required tools
if ! svn --help &>/dev/null; then
	echo "svn not installed!" >&2
	echo "Install it manually: apt-get install subversion" >&2
	echo "Then run this script again">&2
	exit 1
fi

echo Add user lizardfstest
if ! getent passwd lizardfstest > /dev/null; then
	useradd --system --user-group --home /var/lib/lizardfstest lizardfstest
	groupadd fuse
	usermod -a -G fuse lizardfstest
fi

echo Prepare sudo configuration
if ! [[ -d /etc/sudoers.d ]]; then
	# Sudo is not installed bydefault on Debian machines
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
		lizardfstest ALL = NOPASSWD: /bin/sh -c echo\ 1\ >\ /proc/sys/vm/drop_caches
	END
	chmod 0440 /etc/sudoers.d/lizardfstest
fi

echo Prepare /etc/fuse.conf
if ! grep '^[[:blank:]]*user_allow_other' /etc/fuse.conf >/dev/null; then
	echo "user_allow_other" >> /etc/fuse.conf
fi

echo Prepare empty /etc/lizardfs_tests.conf
if ! [[ -f /etc/lizardfs_tests.conf ]]; then
	cat >/etc/lizardfs_tests.conf <<-'END'
		# : ${TEMP_DIR:=/tmp/LizardFS-autotests}
		# : ${LIZARDFS_DISKS:="/mnt/hd1 /mnt/hd2 /mnt/hd3"}
		# : ${LIZARDFS_ROOT:=$HOME/local}
		# : ${FIRST_PORT_TO_USE:=25000}
	END
fi

echo Prepare ramdisk
if ! grep /mnt/ramdisk /etc/fstab >/dev/null; then
	echo "# Ramdisk used in LizardFS tests" >> /etc/fstab
	echo "ramdisk  /mnt/ramdisk  tmpfs  mode=1777,size=2g" >> /etc/fstab
	mkdir -p /mnt/ramdisk
	mount /mnt/ramdisk
fi

set +x
echo Machine configured successfully
