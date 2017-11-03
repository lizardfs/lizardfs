timeout_set 3 hours

writing_thread() {
	local dir=$1
	local thread_id=$2
	local n=0
	local written_bytes=0
	pseudorandom_init $thread_id
	while (( written_bytes < data_size_per_thread )); do
		case $(pseudorandom 1 3) in
			1) local file_size=$(pseudorandom 10 500K);;
			2) local file_size=$(pseudorandom 10 50M);;
			3) local file_size=$(pseudorandom 10 500M);;
		esac
		# Do not overflow more than 200 MB...
		if (( written_bytes + file_size > data_size_per_thread + 200000000 )); then
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

overwriting_thread() {
	local dir=$1
	local thread_id=$2
	pseudorandom_init $thread_id
	for file in $(find "${dir}" -name "empty_${thread_id}.*"); do
		export BLOCK_SIZE=$(pseudorandom 1K 16K)
		MESSAGE="Overwring using block size $BLOCK_SIZE B" expect_success file-overwrite "$file"
	done
}

restarting_thread() {
	pseudorandom_init
	while true; do
		expect_success lizardfs_master_daemon stop
		sleep $(pseudorandom 1 30)
		expect_success lizardfs_master_daemon start
		sleep $(pseudorandom 45 90)
	done
}

spoiling_thread() {
	pseudorandom_init
	while true; do
		for i in 0 1 2; do
			lizardfs_chunkserver_daemon $i stop
			sleep $(pseudorandom 1 30)
			lizardfs_chunkserver_daemon $i start
			sleep 5
		done
	done
}

verifying_thread() {
	local dir=$1
	local thread_id=$2
	for file in $(find "${dir}" -name "*_${thread_id}.*"); do
		expect_success file-validate "$file"
	done
}

# Setup the installation
CHUNKSERVERS=3 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info
dir="${info[mount0]}/dir"
mkdir "$dir"
lizardfs setgoal xor2 "$dir"

# Some configuration goes here
data_size_per_thread=$(parse_si_suffix 3G)
thread_count=10

# Make the master server a bit faulty in this test
( restarting_thread & )

# Generate some files
for i in $(seq 1 $thread_count); do
	writing_thread "$dir" $i &
done
wait

# Some files are empty. Overwrite them using file-generate
for i in $(seq 1 $thread_count); do
	overwriting_thread "$dir" $i &
done
wait

# Spawn the spoiling thread! (parens will daemonize it)
( spoiling_thread & )

# Validate files using many threads
for i in $(seq 1 $thread_count); do
	verifying_thread "$dir" $i &
done
wait
