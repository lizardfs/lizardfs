CHUNKSERVERS=4 \
	CHUNKSERVER_LABELS="0:cs0|1:cs1|2:cs2|3:cs3" \
	MASTER_EXTRA_CONFIG="OPERATIONS_DELAY_INIT = 100000" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
goals="3 xor3"
for goal in $goals; do
	mkdir dir_$goal
	lizardfs setgoal $goal dir_$goal
	echo a > dir_$goal/file
done

list_chunkservers() {
	lizardfs-probe list-chunkservers --porcelain localhost "${info[matocl]}"
}

# Wait for all chunkservers to report chunk creation and see what lizardfs-probe prints then
export MESSAGE="Veryfing chunkservers list with all the chunkservers up"
expect_eventually_prints 7 'list_chunkservers | awk "{chunks += \$3} END {print chunks}"'
cslist=$(list_chunkservers)
expect_awk_finds_no 'NF != 10' "$cslist"

# There should be four entries: four connected servers
expect_equals 4 $(wc -l <<< "$cslist")
expect_equals 4 $(awk -v version="$LIZARDFS_VERSION" '$2 == version' <<< "$cslist" | wc -l)
expect_equals "cs0 cs1 cs2 cs3" "$(awk '{print $10}' <<< "$cslist" | sort | xargs echo)"

# Turn off one chunkserver and see what lizardfs-probe prints now
export MESSAGE="Veryfing chunkservers list with one chunkserver down"
lizardfs_chunkserver_daemon 0 stop
lizardfs_wait_for_ready_chunkservers 3
cslist=$(list_chunkservers)
expect_awk_finds_no 'NF != 10' "$cslist"

# There should be four entries: one disconnected server and three still connected
expect_equals 4 $(wc -l <<< "$cslist")
expect_equals 1 $(awk '$2 == "-"' <<< "$cslist" | wc -l)
expect_equals 3 $(awk -v version="$LIZARDFS_VERSION" '$2 == version' <<< "$cslist" | wc -l)
expect_equals "- cs1 cs2 cs3" "$(awk '{print $10}' <<< "$cslist" | sort | xargs echo)"

# We might have lost 1 or 2 chunks, so there should be 5 or 6 left
expect_matches '^5|6$' "$(awk '$2 != "-" {chunks += $3} END {print chunks}' <<< "$cslist")"
