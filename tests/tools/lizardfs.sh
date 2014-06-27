# Usage: setup_local_empty_lizardfs out_var
#
# Configures and starts master, chunkserver and mounts
# If out_var provided an associative array with name $out_var
# is created and it contains information about the filestystem
setup_local_empty_lizardfs() {
	local use_moosefs=${USE_MOOSEFS:-}
	local use_ramdisk=${USE_RAMDISK:-}
	local use_loop=${USE_LOOP_DISKS:-}
	local number_of_chunkservers=${CHUNKSERVERS:-1}
	local number_of_mounts=${MOUNTS:-1}
	local disks_per_chunkserver=${DISK_PER_CHUNKSERVER:-1}
	local ip_address=$(get_ip_addr)
	local etcdir=$TEMP_DIR/mfs/etc
	local vardir=$TEMP_DIR/mfs/var
	local mntdir=$TEMP_DIR/mnt
	declare -gA lizardfs_info_
	lizardfs_info_[chunkserver_count]=$number_of_chunkservers

	# Prepare directories for LizardFS
	mkdir -p "$etcdir" "$vardir"

	local oldpath="$PATH"
	if [[ $use_moosefs ]]; then
		export PATH="$MOOSEFS_DIR/bin:$MOOSEFS_DIR/sbin:$PATH"
		build_moosefs
	fi

	# Start master
	run_master_server_

	# Prepare the metalogger, so that any test can start it
	prepare_metalogger_

	# Start chunkservers, but first check if he have enough disks
	if [[ ! $use_ramdisk ]]; then
		if [[ $use_loop ]]; then
			local disks=($LIZARDFS_LOOP_DISKS)
		else
			local disks=($LIZARDFS_DISKS)
		fi
		local disks_needed=$((number_of_chunkservers * disks_per_chunkserver))
		local disks_available=${#disks[@]}
		if (( disks_available < disks_needed )); then
			test_fail "Test needs $disks_needed disks"\
					"but only $disks_available (${disks[@]-}) are available"
		fi
	fi
	for ((csid=0 ; csid<number_of_chunkservers; ++csid)); do
		add_chunkserver_ $csid
	done

	# Mount the filesystem
	for ((mntid=0 ; mntid<number_of_mounts; ++mntid)); do
		add_mount_ $mntid
	done

	# Wait for chunkservers
	lizardfs_wait_for_all_ready_chunkservers

	# Return array containing information about the installation
	local out_var=$1
	unset "$out_var"
	declare -gA "$out_var" # Create global associative array, requires bash 4.2
	for key in "${!lizardfs_info_[@]}"; do
		eval "$out_var['$key']='${lizardfs_info_[$key]}'"
	done

	export PATH="$oldpath"
}

# lizardfs_chunkserver_daemon <id> start|stop|restart|kill|tests|isalive|...
lizardfs_chunkserver_daemon() {
	mfschunkserver -c "${lizardfs_info_[chunkserver${1}_config]}" "$2" | cat
	return ${PIPESTATUS[0]}
}

# lizardfs_master_daemon start|stop|restart|kill|tests|isalive|...
lizardfs_master_daemon() {
	mfsmaster -c "${lizardfs_info_[master_cfg]}" "$1" | cat
	return ${PIPESTATUS[0]}
}

# lizardfs_metalogger_daemon start|stop|restart|kill|tests|isalive|...
lizardfs_metalogger_daemon() {
	mfsmetalogger -c "${lizardfs_info_[metalogger_cfg]}" "$1" | cat
	return ${PIPESTATUS[0]}
}

# lizardfs_mount_unmount <id>
lizardfs_mount_unmount() {
	local mount_id=$1
	local mount_dir=${lizardfs_info_[mount${mount_id}]}
	fusermount -u ${mount_dir}
}

# lizardfs_mount_start <id>
lizardfs_mount_start() {
	do_mount_ "$1"
}

# A bunch of private function of this module

create_mfsexports_cfg_() {
	local base="* / rw,alldirs,maproot=0"
	local meta_base="* . rw"
	local additional=${MFSEXPORTS_EXTRA_OPTIONS-}
	local meta_additional=${MFSEXPORTS_META_EXTRA_OPTIONS-}
	if [[ $additional ]]; then
		additional=",$additional"
	fi
	if [[ $meta_additional ]]; then
		meta_additional=",$meta_additional"
	fi
	# general entries
	echo "${base}${additional}"
	echo "${meta_base}${meta_additional}"
	# per-mount entries
	for ((mntid=0 ; mntid<number_of_mounts; ++mntid)); do
		local this_mount_exports_variable="MOUNT_${mntid}_EXTRA_EXPORTS"
		local this_mount_exports=${!this_mount_exports_variable-}
		if [[ ${this_mount_exports} ]]; then
			echo "${base},password=${mntid}${additional},${this_mount_exports}"
		fi
	done
}

create_mfsmaster_cfg_() {
	echo "WORKING_USER = $(id -nu)"
	echo "WORKING_GROUP = $(id -ng)"
	echo "EXPORTS_FILENAME = $etcdir/mfsexports.cfg"
	echo "DATA_PATH = $master_data_path"
	echo "MATOML_LISTEN_PORT = $matoml_port"
	echo "MATOCS_LISTEN_PORT = $matocs_port"
	echo "MATOCL_LISTEN_PORT = $matocl_port"
	echo "${MASTER_EXTRA_CONFIG-}" | tr '|' '\n'
}

run_master_server_() {
	local matoml_port
	local matocl_port
	local matocs_port
	local master_data_path=$vardir/master

	get_next_port_number matoml_port
	get_next_port_number matocl_port
	get_next_port_number matocs_port
	mkdir "$master_data_path"
	echo -n 'MFSM NEW' > "$master_data_path/metadata.mfs"
	create_mfsexports_cfg_ > "$etcdir/mfsexports.cfg"
	create_mfsmaster_cfg_ > "$etcdir/mfsmaster.cfg"

	lizardfs_info_[master_cfg]=$etcdir/mfsmaster.cfg
	lizardfs_info_[master_data_path]=$master_data_path
	lizardfs_info_[matoml]=$matoml_port
	lizardfs_info_[matocl]=$matocl_port
	lizardfs_info_[matocs]=$matocs_port

	lizardfs_master_daemon start
}

create_mfsmetalogger_cfg_() {
	echo "WORKING_USER = $(id -nu)"
	echo "WORKING_GROUP = $(id -ng)"
	echo "DATA_PATH = ${lizardfs_info_[master_data_path]}"
	echo "MASTER_HOST = $(get_ip_addr)"
	echo "MASTER_PORT = ${lizardfs_info_[matoml]}"
	echo "${METALOGGER_EXTRA_CONFIG-}" | tr '|' '\n'
}

prepare_metalogger_() {
	create_mfsmetalogger_cfg_ > "$etcdir/mfsmetalogger.cfg"
	lizardfs_info_[metalogger_cfg]="$etcdir/mfsmetalogger.cfg"
}

create_mfshdd_cfg_() {
	local n=$disks_per_chunkserver
	if [[ $use_ramdisk ]]; then
		local disk_number
		for disk_number in $(seq 1 $n); do
			local disk_dir=$RAMDISK_DIR/hdd_${chunkserver_id}_${disk_number}
			mkdir -pm 777 $disk_dir
			echo $disk_dir
		done
	else
		for d in "${disks[@]:$((n * chunkserver_id)):$n}"; do
			echo "$d"
		done
	fi
}

create_mfschunkserver_cfg_() {
	echo "WORKING_USER = $(id -nu)"
	echo "WORKING_GROUP = $(id -ng)"
	echo "DATA_PATH = $chunkserver_data_path"
	echo "HDD_CONF_FILENAME = $hdd_cfg"
	echo "MASTER_HOST = $ip_address"
	echo "MASTER_PORT = ${lizardfs_info_[matocs]}"
	echo "CSSERV_LISTEN_PORT = $csserv_port"
	echo "${CHUNKSERVER_EXTRA_CONFIG-}" | tr '|' '\n'
}

add_chunkserver_() {
	local chunkserver_id=$1
	local csserv_port
	local chunkserver_data_path=$vardir/chunkserver_$chunkserver_id
	local hdd_cfg=$etcdir/mfshdd_$chunkserver_id.cfg
	local chunkserver_cfg=$etcdir/mfschunkserver_$chunkserver_id.cfg

	get_next_port_number csserv_port
	create_mfshdd_cfg_ > "$hdd_cfg"
	create_mfschunkserver_cfg_ > "$chunkserver_cfg"
	mkdir -p "$chunkserver_data_path"
	mfschunkserver -c "$chunkserver_cfg" start

	lizardfs_info_[chunkserver${chunkserver_id}_port]=$csserv_port
	lizardfs_info_[chunkserver${chunkserver_id}_config]=$chunkserver_cfg
	lizardfs_info_[chunkserver${chunkserver_id}_hdd]=$hdd_cfg
}

create_mfsmount_cfg_() {
	local this_mount_cfg_variable="MOUNT_${1}_EXTRA_CONFIG"
	local this_mount_exports_variable="MOUNT_${1}_EXTRA_EXPORTS"
	echo "mfsmaster=$ip_address"
	echo "mfsport=${lizardfs_info_[matocl]}"
	if [[ ${!this_mount_exports_variable-} ]]; then
		# we want custom exports options, so we need to identify with a password
		echo "mfspassword=${1}"
	fi
	echo "${MOUNT_EXTRA_CONFIG-}" | tr '|' '\n'
	echo "${!this_mount_cfg_variable-}" | tr '|' '\n'
}

do_mount_() {
	local mount_id=$1
	for try in $(seq 1 $max_tries); do
		${lizardfs_info_[mntcall${mount_id}]} && return 0
		echo "Retrying in 1 second ($try/$max_tries)..."
		sleep 1
	done
	echo "Cannot mount in $mount_dir, exiting"
	exit 2
}

add_mount_() {
	local mount_id=$1
	local mount_dir=$mntdir/mfs${mount_id}
	local mount_cfg=$etcdir/mfsmount${mount_id}.cfg
	create_mfsmount_cfg_ ${mount_id} > "$mount_cfg"
	mkdir -p "$mount_dir"
	lizardfs_info_[mount${mount_id}]="$mount_dir"
	lizardfs_info_[mount${mount_id}_config]="$mount_cfg"
	max_tries=30
	fuse_options=""
	for fuse_option in $(echo ${FUSE_EXTRA_CONFIG-} | tr '|' '\n'); do
		fuse_option_name=$(echo $fuse_option | cut -f1 -d'=')
		mfsmount --help |& grep " -o ${fuse_option_name}[ =]" > /dev/null \
				|| test_fail "Your libfuse doesn't support $fuse_option_name flag"
		fuse_options+="-o $fuse_option "
	done
	lizardfs_info_[mntcall$mount_id]="mfsmount -o big_writes -c $mount_cfg $mount_dir $fuse_options"
	do_mount_ ${mount_id}
}

# Some helper functions for tests to manipulate the existing installation

mfs_dir_info() {
	if (( $# != 2 )); then
		echo "Incorrect usage of mfs_dir_info with args $*";
		exit 2;
	fi;
	field=$1
	file=$2
	mfsdirinfo "$file" | grep -w "$field" | grep -o '[0-9]*'
}

find_first_chunkserver_with_chunks_matching() {
	local pattern=$1
	local count=${lizardfs_info_[chunkserver_count]}
	local chunkserver
	for (( chunkserver=0 ; chunkserver < count ; ++chunkserver )); do
		local hdds=$(cat "${lizardfs_info_[chunkserver${chunkserver}_hdd]}")
		if [[ $(find $hdds -name "$pattern") ]]; then
			echo $chunkserver
			return 0
		fi
	done
	return 1
}

# print absolute paths of all chunk files on selected server, one per line
find_chunkserver_chunks() {
	local chunkserver_number=$1
	shift
	local hdds=$(sed -e 's|$|/chunks[A-F0-9][A-F0-9]/|' \
			"${lizardfs_info_[chunkserver${chunkserver_number}_hdd]}")
	if (( $# > 0 )); then
		find $hdds -name "chunk*.mfs" -a "(" "$@" ")"
	else
		find $hdds -name "chunk*.mfs"
	fi
}

# print absolute paths of all chunk files on all servers used in test, one per line
find_all_chunks() {
	local count=${lizardfs_info_[chunkserver_count]}
	local chunkserver
	for (( chunkserver=0 ; chunkserver < count ; ++chunkserver )); do
		find_chunkserver_chunks $chunkserver "$@"
	done
}

# print the number of fully operational chunkservers
lizardfs_ready_chunkservers_count() {
	lizardfs-probe ready-chunkservers-count localhost ${lizardfs_info[matocl]}
}

# lizardfs_wait_for_ready_chunkservers <num> -- waits until <num> chunkservers are fully operational
lizardfs_wait_for_ready_chunkservers() {
	local chunkservers=$1
	local port=${lizardfs_info_[matocl]}
	while [[ "$(lizardfs-probe ready-chunkservers-count localhost $port 2>/dev/null | cat)" \
			!= "$chunkservers" ]]; do
		sleep 0.1
	done
}

lizardfs_wait_for_all_ready_chunkservers() {
	lizardfs_wait_for_ready_chunkservers ${lizardfs_info_[chunkserver_count]}
}

LIZARDFS_BLOCK_SIZE=$((64 * 1024))
LIZARDFS_CHUNK_SIZE=$((1024 * LIZARDFS_BLOCK_SIZE))
