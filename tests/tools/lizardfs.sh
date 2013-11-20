# Usage: setup_local_empty_lizardfs out_var
#
# Configures and starts master, chunkserver and mounts
# If out_var provided an associative array with name $out_var
# is created and it contains information about the filestystem
setup_local_empty_lizardfs() {
	local use_ramdisk=${USE_RAMDISK-}
	local number_of_chunkservers=${CHUNKSERVERS:-1}
	local number_of_mounts=${MOUNTS:-1}
	local disks_per_chunkserver=${DISK_PER_CHUNKSERVER:-1}
	local ip_address=$(get_ip_addr)
	local etcdir=$TEMP_DIR/mfs/etc
	local vardir=$TEMP_DIR/mfs/var
	local mntdir=$TEMP_DIR/mnt
	declare -A lizardfs_info

	# Prepare directories for LizardFS
	mkdir -p "$etcdir" "$vardir"

	# Start master
	run_master_server

	# Start chunkservers, but first check if he have enough disks
	local disks_needed=$((number_of_chunkservers * disks_per_chunkserver))
	local disks_available=$(echo "$LIZARDFS_DISKS" | wc -w)
	if [[ ! $use_ramdisk ]] && (( disks_available < disks_needed )); then
		test_fail "Test needs $disks_needed disks"\
			"but only $disks_available ($LIZARDFS_DISKS) available"
	fi
	for ((csid=0 ; csid<number_of_chunkservers; ++csid)); do
		add_chunkserver $csid
	done

	# Mount the filesystem
	for ((mntid=0 ; mntid<number_of_mounts; ++mntid)); do
		add_mount $mntid
	done

	# Return array containing informationa about the installation
	local out_var=$1
	unset "$out_var"
	declare -gA "$out_var" # Create global associative array, requires bash 4.2
	for key in "${!lizardfs_info[@]}"; do
		eval "$out_var['$key']='${lizardfs_info[$key]}'"
	done
}

create_mfsexports_cfg() {
	echo "* / rw,alldirs,maproot=0"
	echo "* . rw"
}

create_mfsmaster_cfg() {
	echo "WORKING_USER = $(id -nu)"
	echo "WORKING_GROUP = $(id -ng)"
	echo "EXPORTS_FILENAME = $etcdir/mfsexports.cfg"
	echo "DATA_PATH = $master_data_path"
	echo "MATOML_LISTEN_PORT = $matoml_port"
	echo "MATOCS_LISTEN_PORT = $matocs_port"
	echo "MATOCL_LISTEN_PORT = $matocl_port"
	echo "${MASTER_EXTRA_CONFIG-}" | tr '|' '\n'
}

run_master_server() {
	local matoml_port
	local matocl_port
	local matocs_port
	local master_data_path=$vardir/master

	get_next_port_number matoml_port
	get_next_port_number matocl_port
	get_next_port_number matocs_port
	mkdir "$master_data_path"
	echo -n 'MFSM NEW' > "$master_data_path/metadata.mfs"
	create_mfsexports_cfg > "$etcdir/mfsexports.cfg"
	create_mfsmaster_cfg > "$etcdir/mfsmaster.cfg"
	mfsmaster -c "$etcdir/mfsmaster.cfg" start

	lizardfs_info[master_cfg]=$etcdir/mfsmaster.cfg
	lizardfs_info[master_data_path]=$master_data_path
	lizardfs_info[matocl]=$matocl_port
	lizardfs_info[matocs]=$matocs_port
}

create_mfshdd_cfg() {
	local n=$disks_per_chunkserver
	if [[ $use_ramdisk ]]; then
		local disk_number
		for disk_number in $(seq 1 $n); do
			local disk_dir=$RAMDISK_DIR/hdd_${chunkserver_id}_${disk_number}
			mkdir -pm 777 $disk_dir
			echo $disk_dir
		done
	else
		local disks=($LIZARDFS_DISKS)
		for d in "${disks[@]:$((n * chunkserver_id)):$n}"; do
			echo "$d"
		done
	fi
}

create_mfschunkserver_cfg() {
	echo "WORKING_USER = $(id -nu)"
	echo "WORKING_GROUP = $(id -ng)"
	echo "DATA_PATH = $chunkserver_data_path"
	echo "HDD_CONF_FILENAME = $hdd_cfg"
	echo "MASTER_HOST = $ip_address"
	echo "MASTER_PORT = ${lizardfs_info[matocs]}"
	echo "CSSERV_LISTEN_PORT = $csserv_port"
	echo "${CHUNKSERVER_EXTRA_CONFIG-}" | tr '|' '\n'
}

add_chunkserver() {
	local chunkserver_id=$1
	local csserv_port
	local chunkserver_data_path=$vardir/chunkserver_$chunkserver_id
	local hdd_cfg=$etcdir/mfshdd_$chunkserver_id.cfg
	local chunkserver_cfg=$etcdir/mfschunkserver_$chunkserver_id.cfg

	get_next_port_number csserv_port
	create_mfshdd_cfg > "$hdd_cfg"
	create_mfschunkserver_cfg > "$chunkserver_cfg"
	mkdir -p "$chunkserver_data_path"
	mfschunkserver -c "$chunkserver_cfg" start

	lizardfs_info[chunkserver${chunkserver_id}_port]=$csserv_port
	lizardfs_info[chunkserver${chunkserver_id}_config]=$chunkserver_cfg
	lizardfs_info[chunkserver${chunkserver_id}_hdd]=$hdd_cfg
}

create_mfsmount_cfg() {
	echo "mfsmaster=$ip_address"
	echo "mfsport=${lizardfs_info[matocl]}"
	echo "${MOUNT_EXTRA_CONFIG-}" | tr '|' '\n'
}

add_mount() {
	local mount_id=$1
	local mount_dir=$mntdir/mfs$mount_id
	local mount_cfg=$etcdir/mfsmount.cfg
	if ! [[ -f "$mount_cfg" ]]; then
		create_mfsmount_cfg > "$mount_cfg"
		lizardfs_info[mount_config]="$mount_cfg"
	fi
	mkdir -p "$mount_dir"
	lizardfs_info[mount${mount_id}]="$mount_dir"
	max_tries=30
	for try in $(seq 1 $max_tries); do
		mfsmount -c "$mount_cfg" "$mount_dir" && return 0
		echo "Retrying in 1 second ($try/$max_tries)..."
		sleep 1
	done
	echo "Cannot mount in $mount_dir, exiting"
	exit 2
}

function mfs_dir_info() {
    if (( $# != 2 )); then
        echo "Incorrect usage of mfs_dir_info with args $*";
        exit 2;
    fi;
    field=$1
    file=$2
    mfsdirinfo "$file" | grep -w "$field" | grep -o '[0-9]*'
}

LIZARDFS_BLOCK_SIZE=$((64 * 1024))
LIZARDFS_CHUNK_SIZE=$((1024 * LIZARDFS_BLOCK_SIZE))
