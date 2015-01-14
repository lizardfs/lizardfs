# Set up an installation with three disks per chunkserver
USE_RAMDISK=YES \
	CHUNKSERVERS=1 \
	DISK_PER_CHUNKSERVER=3 \
	setup_local_empty_lizardfs info

# Stop daemon, damage 1st disk, start daemon again
assert_success lizardfs_chunkserver_daemon 0 stop
assert_success chmod 000 "$(sort ${info[chunkserver0_hdd]} | head -n 1)"
assert_success lizardfs_chunkserver_daemon 0 start
lizardfs_wait_for_all_ready_chunkservers

# Ensure that disk 1 is damaged and other disks work
list=$(lizardfs_probe_master list-disks | sort)
assert_equals 3 "$(wc -l <<< "$list")"
assert_awk_finds 'NR==1 && $4=="yes"' "$list"
assert_awk_finds 'NR==2 && $4=="no"' "$list"
assert_awk_finds 'NR==3 && $4=="no"' "$list"

# Remove the third disk from the chunkserver
sed -i -e '3s/^/#/' "${info[chunkserver0_hdd]}"
lizardfs_chunkserver_daemon 0 reload
assert_eventually_prints 2 'lizardfs_probe_master list-disks | wc -l'

# Expect that one line disappears from the output of lizardfs-probe
# Ignore columns 6-..., because disk usage might have changed
expected_list=$(head -n2 <<< "$list" | cut -d ' ' -f 1-5)
actual_list=$(lizardfs_probe_master list-disks | sort | cut -d ' ' -f 1-5)
assert_no_diff "$expected_list" "$actual_list"
