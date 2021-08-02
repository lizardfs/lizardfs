function stop_lizardfsXX_chunkservers_from_to {
	local from=$1
	local to=$2
	# stop [from, to)
	for i in $(seq $from $((to - 1))); do
		echo $i
		lizardfsXX_chunkserver_daemon $i stop
	done
}

function stop_lizardfsXX_services {
	local mas_n=$1
	local cs_n=$2
	local cli_n=$3

	assert_equals 1 $mas_n # so far, we always have only 1 legacy master
	for i in $(seq 0 $((cli_n - 1)) ); do
		assert_success lizardfs_mount_unmount $i
	done
	for i in $(seq 0 $((cs_n - 1)) ); do
		assert_success lizardfsXX_chunkserver_daemon $i stop
	done
	assert_success lizardfsXX_master_daemon stop
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

function start_lizardfs_services {
	local mas_n=$1
	local cs_n=$2
	local cli_n=$3
	local mount_cmd="${4:-}"

	for i in $(seq 0 $((mas_n - 1)) ); do
		assert_success lizardfs_master_n $i start
	done
	for i in $(seq 0 $((cs_n - 1)) ); do
		assert_success lizardfs_chunkserver_daemon $i start
	done
	for i in $(seq 0 $((cli_n - 1)) ); do
		assert_success lizardfs_mount_start $i "${mount_cmd}"
	done
	lizardfs_wait_for_all_ready_chunkservers
}

function print_running_services_info {
	echo "Running masters:"
	lizardfs_admin_master info
	echo "Running mounts:"
	lizardfs_admin_master list-mounts
	echo "Running chunkservers"
	lizardfs_admin_master list-chunkservers
}

function check_all_files_readable_and_proper_parts_nr {
	local -n fileCount=$1    # map [filesize -> cnt]
	local expected_number_of_parts=$2
	local replication_speed="${3}"
	local seed_shift=${4:-0}

	for fsize in "${!fileCount[@]}"; do
		local files_n=${fileCount[$fsize]}
		# echo "DEBUG: ${FUNCNAME[0]}: ${files_n}"
		for i in $(seq $files_n); do
			local filesize=$(size_of "${fsize}/${i}")
			local timeout="$((filesize / replication_speed)) seconds"
			check_one_file_part_coverage "${fsize}/${i}" "${expected_number_of_parts}" "${timeout}"
			# and that they're readable
			assert_success validate_file "SEED=$((i + seed_shift))" "${fsize}/${i}"
		done
	done
}

function check_one_dir_replicated {
	local dirname=$1
	local replication_speed=$2
	cd $dirname
	for file in *; do
		[ -f ${file} ] || continue
		echo "Checking if replicated properly, dir: $dirname, file: ${file}"
		local filesize=$(size_of "${file}")
		local timeout="$((filesize / replication_speed)) seconds"
		check_one_file_replicated "${file}" "${timeout}"
	done
	cd -
}

function validate_one_dir {
	local dirname=$1
	local n_files=$2
	local n_overwritten=$(((n_files + 1) / 2))
	cd $dirname
	for i in $(seq 1 $n_overwritten); do
		assert_success validate_file "SEED=$((1000 + i))" "${i}"
	done
	# normal, old files, seed=default
	for i in $(seq $((n_overwritten + 1)) ${n_files}); do
		assert_success validate_file "SEED=${i}" "${i}"
	done
	cd -
}

function wait_for_files_replication {
	local -n fileCount=$1   # [filesize -> cnt]
	local replication_speed=$2
	# echo "DEBUG: ${FUNCNAME[0]}, ${!fileCount[@]}, $replication_speed"

	for fsize in "${!fileCount[@]}"; do
		check_one_dir_replicated "${fsize}" "${replication_speed}"
	done
}

function generate_file() {
	# echo "$(pwd): ${FUNCNAME[0]} params=${@}"
	parametrize_command file-generate "${@}"
}

function validate_file() {
	# echo "$(pwd): ${FUNCNAME[0]} params=${@}"
	parametrize_command file-validate "${@}"
}

function fill_files_info_maps_with_default_values {
	local -n fileCount=$1    # [filesize -> cnt]

	fileCount[10M]=2

	fileCount[55M]=2

	fileCount[63M]=2

	fileCount[64M]=2

	fileCount[65M]=2

	fileCount[100M]=2

	fileCount[1G]=1

	fileCount[2G]=1
}

function generate_one_dir {
	local dirname="$1"
	local filesize="$2"
	local n_files="$3"
	local seed_shift="$4"
	mkdir -p ${dirname}
	cd ${dirname}
	echo "Generating files in directory: ${dirname}"
	for i in $(seq $n_files); do
		generate_file "FILE_SIZE=${filesize}" "SEED=$((i + seed_shift))" "${i}"
	done
	cd -
}

function generate_files_various_filesizes {
	local -n fileCount=${1} # map [filesize -> cnt]
	local seed_shift=${2:-0}
	for fsize in "${!fileCount[@]}"; do
		generate_one_dir $fsize $fsize ${fileCount[$fsize]} $seed_shift
	done
}

function overwrite_half_files {
	# echo "DEBUG: ${FUNCNAME[0]} | <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
	local -n fileCount="${1}" # map [filesize -> cnt]
	local seed_shift=1000
	for fsize in "${!fileCount[@]}"; do
		generate_one_dir $fsize $fsize $(((fileCount[$fsize] + 1) / 2)) $seed_shift
	done
}

function validate_all_files {
	local -n fileCount=${1} # map [filesize -> cnt]
	for fsize in "${!fileCount[@]}"; do
		validate_one_dir $fsize ${fileCount[$fsize]}
	done
}

