assert_program_installed python3 tee

# This timeout is only for launching the environment, it is renewed at each iteration.
timeout_set 40 seconds

get_ec_chunk_part_from_filename() {
	local name=$1
	echo $name | grep -Po '(?<=ec2_).*' | awk -F _ '{print $1}'
}

iteration() {
	timeout_set 300 seconds
	local description="Testing EC(${n},${k}) lost chunks: $lost_chunks"
	echo $description | tee -a $failed_file

	local lost_chunkservers=
	for chunk in $lost_chunks; do
		lost_chunkservers="${lost_chunkservers} ${chunkservers[$chunk]}"
	done
	echo "Losing chunkservers: $lost_chunkservers"

	for chunkserver in $lost_chunkservers; do
		assert_eventually "lizardfs_chunkserver_daemon $chunkserver isalive" "100 seconds"
	done

	for chunkserver in $lost_chunkservers; do
		lizardfs_chunkserver_daemon $chunkserver stop &
	done
	wait

	for chunkserver in $lost_chunkservers; do
		assert_eventually "! lizardfs_chunkserver_daemon $chunkserver isalive" "100 seconds"
	done

	if ! file-validate $directory/file; then
		test_add_failure "Data read from file is different than written. EC(${n},${k}), lost chunks: $lost_chunks"
	fi

	for chunkserver in $lost_chunkservers; do
		lizardfs_chunkserver_daemon $chunkserver start &
	done
	wait

	# Remove from the end of the file if the test passed.
	sed -i "$ d" $failed_file
}

testcase="selected"

case $testcase in
	*all*)
		subsets=$(all_subsets_of_fixed_size $((n + k)) $k)
	;;
	*some*)
		subsets=$(random_subsets_of_fixed_size $((n + k)) $k 10)
		#idx=$(echo $subsets | tr ' ' '\n' | head -n1)
		#echo "Test indices: $idx"
		subsets=$(echo $subsets | tr ' ' '\n' | tail -n +2)

	;;
	*selected*)
		subsets=$(selected_combinations $n $k)
	;;
	# This may be used for manual validation.
	*exact*)
		n=20
		k=5
		subsets=$(echo "5 11 15 17 24" | tr ' ' ',')
	;;
	*)
		exit 1
	;;
esac
cases=$(echo $subsets | wc -w)

goal="ec_${n}_${k}"
echo "Testing EC(${n},${k}) with $cases cases."

CHUNKSERVERS=$((n + k)) \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="READ_AHEAD_KB = 1024|MAX_READ_BEHIND_KB = 2048" \
	MASTER_CUSTOM_GOALS="8 ${goal}: \$ec(${n},${k})" \
	setup_local_empty_lizardfs info

cd ${info[mount0]}

directory="dir_${goal}"
mkdir $directory
lizardfs setgoal -r $goal $directory

FILE_SIZE=64M BLOCK_SIZE=12345 file-generate $directory/file

declare -A chunkservers=() # Chunk {1..(n+k)} -> Chunkserver {0..(n+k-1)} mapping
for ((chunkserver=0; chunkserver<n+k; chunkserver++)) do
	for chunk in $(find_chunkserver_chunks $chunkserver); do
		chunkID=$(get_ec_chunk_part_from_filename $chunk)
		chunkservers[$chunkID]=$chunkserver
	done
done

for lost_chunks in $subsets; do
	lost_chunks=$(echo $lost_chunks | tr ',' ' ')
	iteration
done
