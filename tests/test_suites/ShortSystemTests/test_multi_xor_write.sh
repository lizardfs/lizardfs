for_chunkservers() {
	operation=${1}
	shift
	for csid in "${@}"; do
		mfschunkserver -c ${info[chunkserver${csid}_config]} "${operation}" &
	done
	wait
	if [[ $operation == stop ]]; then
		nr_of_running_chunkservers=$((nr_of_running_chunkservers - $#))
	elif [[ $operation == start ]]; then
		nr_of_running_chunkservers=$((nr_of_running_chunkservers + $#))
	else
		test_fail "Wrong branch"
	fi
	lizardfs_wait_for_ready_chunkservers $nr_of_running_chunkservers
}

timeout_set "1 minute"
CHUNKSERVERS=8 \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|OPERATIONS_DELAY_INIT = 0|ACCEPTABLE_DIFFERENCE = 10.0|DISABLE_CHUNKS_DEL = 1" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

nr_of_running_chunkservers=8
cd ${info[mount0]}

# Produce first version chunks
dd if=/dev/zero of=file bs=1k count=5k
lizardfs setgoal xor2 file
while (( $(lizardfs fileinfo file | grep -c copy) < 4 )); do # 1 [goal1] + 3 [xor2]
	sleep 1
done
lizardfs setgoal xor3 file
while (( $(lizardfs fileinfo file | grep -c copy) < 8 )); do # 1 [goal1] + 3 [xor2] + 4 [xor3]
	sleep 1
done
sleep 2
# Overwrite the file
file-overwrite file
# Stop all chunkservers
for_chunkservers stop {0..7}

cs_list=($(find_first_chunkserver_with_chunks_matching "chunk_????????????????_????????.???"))
for_chunkservers start ${cs_list[@]}
MESSAGE="Validating goal 1 chunk" expect_success file-validate file
for_chunkservers stop ${cs_list[@]}

cs_list[0]=$(find_first_chunkserver_with_chunks_matching "chunk_xor_1_of_2*.???")
cs_list[1]=$(find_first_chunkserver_with_chunks_matching "chunk_xor_2_of_2*.???")
for_chunkservers start ${cs_list[@]}
MESSAGE="Validating xor2 parts of chunk" file-validate file

for_chunkservers stop ${cs_list[0]}
cs_list[0]=$(find_first_chunkserver_with_chunks_matching "chunk_xor_parity_of_2*.???")
for_chunkservers start ${cs_list[0]}
MESSAGE="Validating xor2 parity of chunk" file-validate file
for_chunkservers stop ${cs_list[@]}

cs_list[0]=$(find_first_chunkserver_with_chunks_matching "chunk_xor_1_of_3*.???")
cs_list[1]=$(find_first_chunkserver_with_chunks_matching "chunk_xor_2_of_3*.???")
cs_list[2]=$(find_first_chunkserver_with_chunks_matching "chunk_xor_3_of_3*.???")
for_chunkservers start ${cs_list[@]}
MESSAGE="Validating xor3 parts of chunk" file-validate file

for_chunkservers stop ${cs_list[2]}
cs_list[2]=$(find_first_chunkserver_with_chunks_matching "chunk_xor_parity_of_3*.???")
for_chunkservers start ${cs_list[2]}
MESSAGE="Validating xor3 parity of chunk" file-validate file
