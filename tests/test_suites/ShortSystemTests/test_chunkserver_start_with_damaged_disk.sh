# Set up an installation with three disks per chunkserver
USE_RAMDISK=YES \
	CHUNKSERVERS=1 \
	DISK_PER_CHUNKSERVER=3 \
	setup_local_empty_lizardfs info

# Stop daemon, damage 1st disk, start daemon again
assert_success lizardfs_chunkserver_daemon 0 stop
assert_success chmod 000 "$(sort ${info[chunkserver0_hdd]} | head -n 1)"
assert_success lizardfs_chunkserver_daemon 0 start

# Ensure that disk 1 is damaged and other disks work
list=$(lizardfs_probe_master list-disks | sort)
assert_awk_finds 'NR==1 && $4=="yes"' "$list"
assert_awk_finds 'NR==2 && $4=="no"' "$list"
assert_awk_finds 'NR==3 && $4=="no"' "$list"
