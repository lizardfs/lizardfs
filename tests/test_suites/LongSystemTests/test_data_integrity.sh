timeout_set 1 hour

CHUNKSERVERS=2 \
	MOUNT_EXTRA_CONFIG="lfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd ${info[mount0]}

test_worker() {
	local min_size=$1
	local max_size=$2
	local seed=$(($(parse_si_suffix $min_size) + $(parse_si_suffix $max_size)))
	pseudorandom_init $seed
	while ! test_frozen; do
		local block_size=$(pseudorandom 1024 128K)
		local file_size=$(pseudorandom $min_size $max_size)
		local file=$(unique_file)
		echo FILE_SIZE=$file_size BLOCK_SIZE=$block_size file-generate "$file"
		if FILE_SIZE=$file_size BLOCK_SIZE=$block_size file-generate "$file"; then
			if ! file-validate "$file"; then
				test_add_failure "Invalid data: block $block_size, size $file_size"
			fi
			lfssettrashtime 0 "$file" &>/dev/null
			rm -f "$file"
		else
			test_add_failure "file-validate failed: block $block_size, size $file_size"
		fi
	done
}

# Run 5 threads writing and reading files of different sizes
# and let them run for 45 minutes
test_worker 8  1K  &
test_worker 8  5M  &
test_worker 1M 50M &
test_worker 1M 1G  &
test_worker 1G 6G  &
sleep 45m

# Stop workers by killing
test_freeze_result
sleep 1
killall -9 file-generate || true
killall -9 file-validate || true
sleep 1
