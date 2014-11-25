#!/bin/bash

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

You need root permissions to run this script
EOF
	exit 1
}

if [[ $1 != setup ]]; then
	usage >&2
fi
shift

# Make this script safe and bug-free ;)
set -eux

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
if ! grep lizardfstest_loop /etc/fstab >/dev/null; then
	i=0
	devices=6
	loops=()
	echo "# Loop devices used in LizardFS tests" >> /etc/fstab
	for disk in "$@" "$@" "$@" "$@" "$@" "$@"; do
		if (( i == devices )); then
			break
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
		loops+=(/mnt/lizardfstest_loop_$i)
		(( ++i ))
	done
	if (( i > 0 )) ; then
		echo ': ${LIZARDFS_LOOP_DISKS:="'"${loops[*]}"'"}'
	else
		echo ': ${LIZARDFS_LOOP_DISKS:=}'
	fi >> /etc/lizardfs_tests.conf
fi

set +x
echo Machine configured successfully
