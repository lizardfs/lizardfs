CHUNKSERVERS=4 \
	DISK_PER_CHUNKSERVER=3 \
	MASTER_EXTRA_CONFIG="OPERATIONS_DELAY_INIT = 100000" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
for goal in xor3 3; do
	mkdir dir_$goal
	lizardfs setgoal $goal dir_$goal
	FILE_SIZE=60M file-generate dir_$goal/file
done

disks=$(lizardfs-probe list-disks --porcelain localhost "${info[matocl]}")
expect_equals 12 $(wc -l <<< "$disks")
for i in {0..3}; do
	cs_data="$(grep ":${info[chunkserver${i}_port]} " <<< "$disks")"
	expect_equals 3 $(wc -l <<< "$cs_data")
	cat "${info[chunkserver${i}_hdd]}" | while read path; do
		expect_equals 1 $(awk -v disk="${path}/" '$2 == disk' <<< "$cs_data" | wc -l)
	done
done
expect_equals "no no no 0 0" "$(cut -d' ' -f 3-7 <<< "$disks" | uniq)"
expect_equals 12 "$(awk '$8 > 0' <<< "$disks" | wc -l)"
expect_equals 7 $(awk '{chunks += $NF} END {print chunks}' <<< "$disks")
