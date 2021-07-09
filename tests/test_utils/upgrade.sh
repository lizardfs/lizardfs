function stop_lizardfsXX_chunkservers {
	echo "a"
}

function stop_lizardfsXX_services {
	local mas_n=$1
	local cli_n=$2
	local cs_n=$3

	assert_equals 1 $mas_n # so far, we always have only 1 legacy master
	for i in $(seq 0 $((cs_n - 1)) ); do
		assert_success lizardfsXX_chunkserver_daemon $i stop
	done
	for i in $(seq 0 $((cli_n - 1)) ); do
		assert_success lizardfs_mount_unmount $i
	done
	assert_success lizardfsXX_master_daemon stop
}

function start_lizardfs_services {
	local mas_n=$1
	local cli_n=$2
	local cs_n=$3

	for i in $(seq 0 $((mas_n - 1)) ); do
		assert_success lizardfs_master_n $i start
	done
	for i in $(seq 0 $((cli_n - 1)) ); do
		assert_success lizardfs_mount_start $i
	done
	for i in $(seq 0 $((cs_n - 1)) ); do
		assert_success lizardfs_chunkserver_daemon $i start
	done
	lizardfs_wait_for_all_ready_chunkservers
}

function wait_for_files_replication {
	local -n sizes_map=$1   # [filesize -> cnt]
	local -n chunk_n_map=$2 # [filesize -> chunks_nr]
	local cs_n=$3
	local timeout=$4

	echo "DAFAQ"
	echo ${!sizes_map[@]}
	echo ${!chunk_n_map[@]}
	echo $cs_n
	echo $timeout
	for fsize in "${!sizes_map[@]}"; do
		local files_n=${sizes_map[$fsize]}
		local chunk_n=${chunk_n_map[$fsize]}
		local expected=$((chunk_n * cs_n))
		echo $fsize $files_n
		for i in $(seq $files_n); do
			local file=${fsize}/${i}
			assert_eventually '[[ $(lizardfs fileinfo $file | grep "part" | wc -l) == $expected ]]' "$timeout"
			echo $file $(lizardfs fileinfo $file | grep "part" | wc -l)
		done
	done
}

function validate_all_files {
	echo "a"
}

function fill_files_info_maps_with_default_values {
	local -n sizes_map=$1    # [filesize -> cnt]
	local -n chunk_n_map=$2 # [filesize -> chunks_nr]

	sizes_map[10M]=2
	chunk_n_map[10M]=1

	sizes_map[55M]=1
	chunk_n_map[55M]=1

	sizes_map[63M]=1
	chunk_n_map[63M]=1

	sizes_map[64M]=1
	chunk_n_map[64M]=1

	sizes_map[65M]=1
	chunk_n_map[65M]=2

	sizes_map[100M]=2
	chunk_n_map[100M]=2
	#sizes_map[1G]=1
	#sizes_map[2G]=1
}

function generate_files_various_filesizes {
	local -n sizes_map=$1 # map [filesize -> cnt]
	for fsize in "${!sizes_map[@]}"; do
		echo $fsize
		_generate_one_dir $fsize ${sizes_map[$fsize]}
	done
}

function _generate_one_dir {
	local fsize=$1
	local files_n=$2
	mkdir $fsize
	cd $fsize
	for i in $(seq $files_n); do
		FILE_SIZE=$fsize file-generate $i
	done
	cd -
}

function stop_lizardfsXX_chunkservers_from_to {
	local from=$1
	local to=$2
	# stop [from, to)
	for i in $(seq $from $((to - 1))); do
		echo $i
		lizardfsXX_chunkserver_daemon $i stop
	done
}

# $1 = stop/start, etc. Start/stop chunkservers [from, to)
function lizardfs_chunkservers_from_to {
	local cmd=$1
	local from=$2
	local to=$3
	for i in $(seq $from $((to - 1)) ); do
		lizardfs_chunkserver_daemon $i $cmd
	done
}

function check_all_files_readable_and_proper_parts_nr {
	local -n sizes_map=$1    # map [filesize -> cnt]
	local -n chunk_n_map=$2  # map [filesize -> chunk_nr]
	local cs_up_n=$3         # nr of chunkservers running

	echo "BALBALVABLAB"
	echo ${!sizes_map[@]}
	echo ${!chunk_n_map[@]}
	echo $cs_up_n
	for fsize in "${!sizes_map[@]}"; do
		local files_n=${sizes_map[$fsize]}
		local chunk_n=${chunk_n_map[$fsize]}
		# Check if all files of given filesize have chunk_n*cs_up_n parts
		for i in $(seq $files_n); do
			local expected=$((chunk_n * cs_up_n))
			local actual=$(lizardfs fileinfo ${fsize}/${i} | grep "part" | wc -l)
			assert_equals $expected $actual

			echo ${fsize}/${i} $expected $actual
			# and that they're readable
			assert_success file-validate ${fsize}/${i}
		done
	done
}

function print_running_services_info {
	echo "Running masters:"
	lizardfs_admin_master info
	echo "Running mounts:"
	lizardfs_admin_master list-mounts
	echo "Running chunkservers"
	lizardfs_admin_master list-chunkservers
}

