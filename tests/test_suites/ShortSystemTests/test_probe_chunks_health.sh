timeout_set 1 minute

CHUNKSERVERS=4 \
	MASTER_EXTRA_CONFIG="OPERATIONS_DELAY_INIT = 100000" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

# Create one chunk in each of four goals
goals="2 3 xor2 xor3"
cd "${info[mount0]}"
for goal in $goals; do
	mkdir dir_$goal
	lizardfs setgoal $goal dir_$goal
	echo a > dir_$goal/file
done

export MESSAGE="Veryfing health report with all the chunkservers up"
health4=$(lizardfs-probe chunks-health --porcelain localhost "${info[matocl]}")
expect_equals 4 $(awk '/AVA/ {chunks += ($3 + $4 + $5)} END {print chunks}' <<< "$health4")
for goal in $goals; do
	expect_awk_finds "/AVA $goal 1 0 0/" "$health4"
done
expect_awk_finds_no '/AVA/ && $4 > 0' "$health4"
expect_awk_finds_no '/AVA/ && $5 > 0' "$health4"
expect_awk_finds_no '/DEL [xor0-9]+ [0-9]+ .*[1-9]/' "$health4"
expect_awk_finds_no '/REP [xor0-9]+ [0-9]+ .*[1-9]/' "$health4"

# Choose chunkservers to stop in the way, that the chunkserver with one copy
# of the chunk in goal 2 will be turned off as the first one
chunk=$(lizardfs fileinfo dir_2/file | awk '/chunk 0:/{print $3}')
csid1=$(find_first_chunkserver_with_chunks_matching "*$chunk*")
csid2=$(( (csid1 + 1) % 4 ))
csid3=$(( (csid2 + 1) % 4 ))
csid4=$(( (csid3 + 1) % 4 ))

export MESSAGE="Veryfing health report one chunkserver down and chunk with goal 2 endangered"
lizardfs_chunkserver_daemon $csid1 stop
lizardfs_wait_for_ready_chunkservers 3
health3=$(lizardfs-probe chunks-health --porcelain localhost "${info[matocl]}")
expect_equals 4 $(awk '/AVA/ {chunks += ($3 + $4 + $5)} END {print chunks}' <<< "$health3")
expect_awk_finds '/AVA 2 0 1 0/' "$health3"
expect_awk_finds '/AVA xor3 0 1 0/' "$health3"
expect_awk_finds_no '/AVA/ && $5 > 0' "$health3"
expect_awk_finds_no '/AVA 3/ && $4 > 0' "$health3"
expect_awk_finds '/REP 2 0 1 [ 0]+$/' "$health3"
expect_awk_finds '/REP xor3 0 1 [ 0]+$/' "$health3"
expect_awk_finds_no '/DEL [xor0-9]+ [0-9]+ .*[1-9]/' "$health3"

export MESSAGE="Veryfing health report with two out of four chunkservers down"
lizardfs_chunkserver_daemon $csid2 stop
lizardfs_wait_for_ready_chunkservers 2
health2=$(lizardfs-probe chunks-health --porcelain localhost "${info[matocl]}")
expect_equals 4 $(awk '/AVA/ {chunks += ($3 + $4 + $5)} END {print chunks}' <<< "$health2")
expect_awk_finds '/AVA xor3 0 0 1/' "$health2"
expect_awk_finds_no '/REP xor/ && $3 > 0' "$health2"
expect_awk_finds '/REP 2 0 (0 1|1 0) [ 0]+$/' "$health2"
expect_awk_finds '/REP xor3 0 0 1 [ 0]+$/' "$health2"
expect_awk_finds_no '/DEL [xor0-9]+ [0-9]+ .*[1-9]/' "$health2"

export MESSAGE="Veryfing health report with three out of four chunkservers down"
lizardfs_chunkserver_daemon $csid3 stop
lizardfs_wait_for_ready_chunkservers 1
health1=$(lizardfs-probe chunks-health --porcelain localhost "${info[matocl]}")
expect_equals 4 $(awk '/AVA/ {chunks += ($3 + $4 + $5)} END {print chunks}' <<< "$health1")
expect_awk_finds_no '/AVA/ && $3 > 0' "$health1"
expect_awk_finds_no '/AVA xor/ && $4 > 0' "$health1"
expect_awk_finds_no '/REP/ && $3 > 0' "$health1"
expect_awk_finds '/REP 2 0 (0 1|1 0) [ 0]+$/' "$health1"
expect_awk_finds '/REP xor3 0 0 0 1 [ 0]+$/' "$health1"
expect_awk_finds_no '/DEL [xor0-9]+ [0-9]+ .*[1-9]/' "$health1"

export MESSAGE="Veryfing health report with all chunkservers down"
lizardfs_chunkserver_daemon $csid4 stop
lizardfs_wait_for_ready_chunkservers 0
health0=$(lizardfs-probe chunks-health --porcelain localhost "${info[matocl]}")
expect_equals 4 $(awk '/AVA/ {chunks += ($3 + $4 + $5)} END {print chunks}' <<< "$health0")
expect_awk_finds_no '/AVA/ && $3 > 0' "$health0"
expect_awk_finds_no '/AVA/ && $4 > 0' "$health0"
expect_awk_finds_no '/REP/ && $3 > 0' "$health0"
expect_awk_finds_no '/REP/ && $4 > 0' "$health0"
expect_awk_finds '/REP 2 0 0 1 0 0 /' "$health0"
expect_awk_finds '/REP 3 0 0 0 1 0 /' "$health0"
expect_awk_finds '/REP xor2 0 0 0 1 0 /' "$health0"
expect_awk_finds '/REP xor3 0 0 0 0 1 /' "$health0"
expect_awk_finds_no '/DEL [xor0-9]+ [0-9]+ .*[1-9]/' "$health0"

export MESSAGE="Veryfing health report with one out of four chunkservers up again"
lizardfs_chunkserver_daemon $csid4 start
lizardfs_wait_for_ready_chunkservers 1
expect_equals "$health1" "$(lizardfs-probe chunks-health --porcelain localhost "${info[matocl]}")"

export MESSAGE="Veryfing health report with two out of four chunkservers up again"
lizardfs_chunkserver_daemon $csid3 start
lizardfs_wait_for_ready_chunkservers 2
expect_equals "$health2" "$(lizardfs-probe chunks-health --porcelain localhost "${info[matocl]}")"

export MESSAGE="Veryfing health report with three out of four chunkservers up again"
lizardfs_chunkserver_daemon $csid2 start
lizardfs_wait_for_ready_chunkservers 3
expect_equals "$health3" "$(lizardfs-probe chunks-health --porcelain localhost "${info[matocl]}")"

export MESSAGE="Veryfing health report with all the chunkservers up again"
lizardfs_chunkserver_daemon $csid1 start
lizardfs_wait_for_ready_chunkservers 4
expect_equals "$health4" "$(lizardfs-probe chunks-health --porcelain localhost "${info[matocl]}")"
