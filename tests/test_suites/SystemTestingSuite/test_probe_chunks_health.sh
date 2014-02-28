timeout_set 1 minute

CHUNKSERVERS=4 \
	MASTER_EXTRA_CONFIG="REPLICATIONS_DELAY_INIT = 100000" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

# Create one chunk in each of four goals
goals="2 3 xor2 xor3"
cd "${info[mount0]}"
for goal in $goals; do
	mkdir dir_$goal
	mfssetgoal $goal dir_$goal
	echo a > dir_$goal/file
done

export MESSAGE="Veryfing health report with all the chunkservers up"
health4=$(lizardfs-probe chunks-health localhost "${info[matocl]}" --porcelain)
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
chunk=$(mfsfileinfo dir_2/file | awk '/chunk 0:/{print $3}')
csid1=$(find_first_chunkserver_with_chunks_matching "*$chunk*")
csid2=$(( (csid1 + 1) % 4 ))
csid3=$(( (csid2 + 1) % 4 ))
csid4=$(( (csid3 + 1) % 4 ))

export MESSAGE="Veryfing health report one chunkserver down and chunk with goal 2 endangered"
mfschunkserver -c "${info[chunkserver${csid1}_config]}" stop
lizardfs_wait_for_ready_chunkservers 3
health3=$(lizardfs-probe chunks-health localhost "${info[matocl]}" --porcelain)
expect_equals 4 $(awk '/AVA/ {chunks += ($3 + $4 + $5)} END {print chunks}' <<< "$health3")
expect_awk_finds '/AVA 2 0 1 0/' "$health3"
expect_awk_finds '/AVA xor3 0 1 0/' "$health3"
expect_awk_finds_no '/AVA/ && $5 > 0' "$health3"
expect_awk_finds_no '/AVA 3/ && $4 > 0' "$health3"
expect_awk_finds '/REP 2 0 1 [ 0]+$/' "$health3"
expect_awk_finds '/REP xor3 0 1 [ 0]+$/' "$health3"
expect_awk_finds_no '/DEL [xor0-9]+ [0-9]+ .*[1-9]/' "$health3"

export MESSAGE="Veryfing health report with two out of four chunkservers down"
mfschunkserver -c "${info[chunkserver${csid2}_config]}" stop
lizardfs_wait_for_ready_chunkservers 2
health2=$(lizardfs-probe chunks-health localhost "${info[matocl]}" --porcelain)
expect_equals 4 $(awk '/AVA/ {chunks += ($3 + $4 + $5)} END {print chunks}' <<< "$health2")
expect_awk_finds '/AVA xor3 0 0 1/' "$health2"
expect_awk_finds_no '/REP xor/ && $3 > 0' "$health2"
expect_awk_finds '/REP 2 0 (0 1|1 0) [ 0]+$/' "$health2"
expect_awk_finds '/REP xor3 0 0 1 [ 0]+$/' "$health2"
expect_awk_finds_no '/DEL [xor0-9]+ [0-9]+ .*[1-9]/' "$health2"

export MESSAGE="Veryfing health report with three out of four chunkservers down"
mfschunkserver -c "${info[chunkserver${csid3}_config]}" stop
lizardfs_wait_for_ready_chunkservers 1
health1=$(lizardfs-probe chunks-health localhost "${info[matocl]}" --porcelain)
expect_equals 4 $(awk '/AVA/ {chunks += ($3 + $4 + $5)} END {print chunks}' <<< "$health1")
expect_awk_finds_no '/AVA/ && $3 > 0' "$health1"
expect_awk_finds_no '/AVA xor/ && $4 > 0' "$health1"
expect_awk_finds_no '/REP/ && $3 > 0' "$health1"
expect_awk_finds '/REP 2 0 (0 1|1 0) [ 0]+$/' "$health1"
expect_awk_finds '/REP xor3 0 0 0 1 [ 0]+$/' "$health1"
expect_awk_finds_no '/DEL [xor0-9]+ [0-9]+ .*[1-9]/' "$health1"

export MESSAGE="Veryfing health report with all chunkservers down"
mfschunkserver -c "${info[chunkserver${csid4}_config]}" stop
lizardfs_wait_for_ready_chunkservers 0
health0=$(lizardfs-probe chunks-health localhost "${info[matocl]}" --porcelain)
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
mfschunkserver -c "${info[chunkserver${csid4}_config]}" start
lizardfs_wait_for_ready_chunkservers 1
expect_equals "$health1" "$(lizardfs-probe chunks-health localhost "${info[matocl]}" --porcelain)"

export MESSAGE="Veryfing health report with two out of four chunkservers up again"
mfschunkserver -c "${info[chunkserver${csid3}_config]}" start
lizardfs_wait_for_ready_chunkservers 2
expect_equals "$health2" "$(lizardfs-probe chunks-health localhost "${info[matocl]}" --porcelain)"

export MESSAGE="Veryfing health report with three out of four chunkservers up again"
mfschunkserver -c "${info[chunkserver${csid2}_config]}" start
lizardfs_wait_for_ready_chunkservers 3
expect_equals "$health3" "$(lizardfs-probe chunks-health localhost "${info[matocl]}" --porcelain)"

export MESSAGE="Veryfing health report with all the chunkservers up again"
mfschunkserver -c "${info[chunkserver${csid1}_config]}" start
lizardfs_wait_for_ready_chunkservers 4
expect_equals "$health4" "$(lizardfs-probe chunks-health localhost "${info[matocl]}" --porcelain)"
