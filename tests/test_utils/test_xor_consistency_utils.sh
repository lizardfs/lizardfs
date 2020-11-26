: ${MASTER_RESTARTING_LOOP_FILE:=${TEMP_DIR}/.master_restarting_loop_flag}
: ${CHUNKSERVERS_RESTARTING_LOOP_FILE:=${TEMP_DIR}/.chunkservers_restarting_loop_flag}

writing_loop_thread() {
	local dir=$1
	local thread_id=$2
	local n=0
	local written_bytes=0
	local data_size_to_write="${DATA_SIZE_PER_THREAD:-1024}"
	pseudorandom_init $thread_id
	while (( written_bytes < data_size_to_write )); do
		case $(pseudorandom 1 3) in
			1) local file_size=$(pseudorandom 10 500K);;
			2) local file_size=$(pseudorandom 500K 50M);;
			3) local file_size=$(pseudorandom 50M 500M);;
		esac
		# Do not overflow more than 200 MB...
		if (( written_bytes + file_size > data_size_to_write + 200000000 )); then
			continue
		fi
		# Some files will be empty (ie. full of 0's), some will have generated data
		local file_type=$(shuf -n1 -e empty generated)
		local file_name="${dir}/${file_type}_${thread_id}.$((n++))_size_${file_size}"
		if [[ $file_type == generated ]]; then
			local block_size=$(pseudorandom 1K 16K)
			BLOCK_SIZE=$block_size FILE_SIZE=$file_size expect_success file-generate "$file_name"
		else
			if ! head -c "$file_size" /dev/zero > "$file_name"; then
				test_add_failure "Generating $file_name from /dev/zero failed"
			fi
		fi
		: $(( written_bytes += file_size ))
	done
}

overwriting_loop_thread() {
	local dir=$1
	local thread_id=$2
	pseudorandom_init $thread_id
	for file in $(find "${dir}" -name "empty_${thread_id}.*"); do
		export BLOCK_SIZE=$(pseudorandom 1K 16K)
		MESSAGE="Overwring using block size $BLOCK_SIZE B" expect_success file-overwrite "$file"
	done
}

verifying_loop_thread() {
	local dir=$1
	local thread_id=$2
	local only_generated_files=${3:-}
	if [ "$only_generated_files" == 'only_generated_files' ]; then
		for file in $(find "${dir}" -name "generated_${thread_id}.*"); do
			expect_success file-validate "$file"
		done
	else
		for file in $(find "${dir}" -name "*_${thread_id}.*"); do
			expect_success file-validate "$file"
		done
	fi
}

master_restarting_loop() {
	touch ${MASTER_RESTARTING_LOOP_FILE}
	pseudorandom_init
	while [ -e ${MASTER_RESTARTING_LOOP_FILE} ]; do
		expect_success lizardfs_master_daemon stop
		sleep $(pseudorandom 1 30)
		expect_success lizardfs_master_daemon start
		sleep $(pseudorandom 45 90)
	done
	echo "master_restarting_loop stopped"
}

chunkservers_restarting_loop() {
	local chunkservers_num=$1
	touch ${CHUNKSERVERS_RESTARTING_LOOP_FILE}
	pseudorandom_init
	while true; do
		for i in $(seq 0 $((chunkservers_num - 1))); do
			if [ ! -e ${CHUNKSERVERS_RESTARTING_LOOP_FILE} ]; then
				break 2
			fi
			lizardfs_chunkserver_daemon $i stop
			sleep $(pseudorandom 1 30)
			lizardfs_chunkserver_daemon $i start
			sleep 5
		done
	done
	echo "chunkservers_restarting_loop stopped"
}

stop_master_restarting_thread() {
	rm -f ${MASTER_RESTARTING_LOOP_FILE}
}

stop_chunkservers_restarting_thread() {
	rm -f ${CHUNKSERVERS_RESTARTING_LOOP_FILE}
}
