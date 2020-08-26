#!/usr/bin/env bash

usage() {
	cat >&2 <<EOF
Usage: $0 confirm

This script reverts the changes applied by the setup_machine.sh script.
It may be useful for debugging the setup_machine.sh script on new operating systems.
It should be used __only__ by advanced users, who know what this script does.
You need root permissions to run this script.
EOF
	exit 1
}

if [[ $1 != confirm ]]; then
	usage >&2
fi
shift

set -eux

sudoers="/etc/sudoers.d/lizardfstest"

for username in lizardfstest_{0..9}; do
	if getent passwd $username > /dev/null; then
		userdel $username
	fi
done

if getent passwd lizardfstest > /dev/null; then
	userdel lizardfstest
fi

if getent group lizardfstest > /dev/null; then
	groupdel lizardfstest
fi

rm -f /etc/sudoers.d/lizardfstest
rm -rf /var/lib/lizardfstest
rm -f /etc/security/limits.d/10-lizardfstests.conf
rm -f /etc/lizardfs_tests.conf

mountpoint /mnt/ramdisk && umount /mnt/ramdisk
rm -rf /mnt/ramdisk

sed -i -e "/### Reload pam limits on sudo - necessary for lizardfs tests. ###/d" /etc/pam.d/sudo
sed -i -e "/session required pam_limits.so/d" /etc/pam.d/sudo

sed -i -e "/# Ramdisk used in LizardFS tests/d" /etc/fstab
sed -i -e "\[ramdisk  /mnt/ramdisk  tmpfs  mode=1777,size=2g[d" /etc/fstab
sed -i -e "/# Loop devices used in LizardFS tests/d" /etc/fstab

tempfile=$(mktemp -p /tmp revert_setup_machine-etc-fstab.XXXXXXXXXX)
cp /etc/fstab $tempfile

cat $tempfile | grep "lizardfstest_loop" | while read -r line ; do
	file_system=$(echo $line | awk '{print $1}')
	file_system_directory=$(dirname $file_system)
	mount_point=$(echo $line | awk '{print $2}')

	mountpoint $mount_point && umount -d $mount_point
	[ -d "$mount_point" ] && rmdir --ignore-fail-on-non-empty --parents $mount_point
	rm -f $file_system
	[ -d "$file_system_directory" ] && rmdir --ignore-fail-on-non-empty --parents $file_system_directory
	sed -i -e "@$line@d" /etc/fstab
done

rm $tempfile
set +x
echo "Reverted successfully"
