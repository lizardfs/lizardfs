# Usage: setup_local_empty_lizardfs out_var
# Configures and starts master, chunkserver and mounts
# If out_var provided an associative array with name $out_var
# is created and it contains information about the filestystem
setup_local_empty_lizardfs() {
	local use_moosefs=${USE_MOOSEFS:-}
	local use_ramdisk=${USE_RAMDISK:-}
	local use_loop=${USE_LOOP_DISKS:-}
	local number_of_masterservers=${MASTERSERVERS:-1}
	local number_of_chunkservers=${CHUNKSERVERS:-1}
	local number_of_mounts=${MOUNTS:-1}
	local disks_per_chunkserver=${DISK_PER_CHUNKSERVER:-1}
	local auto_shadow_master=${AUTO_SHADOW_MASTER:-YES}
	local cgi_server=${CGI_SERVER:-NO}
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

	# Prepare configuration for metadata servers
	prepare_common_metadata_server_files_
	add_metadata_server_ 0 "master"
	for ((msid=1 ; msid<number_of_masterservers; ++msid)); do
		add_metadata_server_ $msid "shadow"
	done
	lizardfs_info_[current_master]=0
	lizardfs_info_[master_cfg]=${lizardfs_info_[master0_cfg]}
	lizardfs_info_[master_data_path]=${lizardfs_info_[master0_data_path]}
	lizardfs_info_[masterserver_count]=$number_of_masterservers

	# Start one masterserver with personality master
	lizardfs_master_daemon start

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

	export PATH="$oldpath"

	# Add shadow master if not present (and not disabled); wait for it to synchronize
	if [[ $auto_shadow_master == YES && $number_of_masterservers == 1 ]]; then
		add_metadata_server_ auto "shadow"
		lizardfs_master_n auto start
		assert_eventually 'lizardfs_shadow_synchronized auto'
	fi

	if [[ $cgi_server == YES ]]; then
		add_cgi_server_
	fi

	# Wait for chunkservers (use lizardfs-probe only for LizardFS -- MooseFS doesn't support it)
	if [[ ! $use_moosefs ]]; then
		lizardfs_wait_for_ready_chunkservers $number_of_chunkservers
	else
		sleep 3 # A reasonable fallback
	fi

	# Return array containing information about the installation
	local out_var=$1
	unset "$out_var"
	declare -gA "$out_var" # Create global associative array, requires bash 4.2
	for key in "${!lizardfs_info_[@]}"; do
		eval "$out_var['$key']='${lizardfs_info_[$key]}'"
	done
}

# lizardfs_chunkserver_daemon <id> start|stop|restart|kill|tests|isalive|...
lizardfs_chunkserver_daemon() {
	mfschunkserver -c "${lizardfs_info_[chunkserver${1}_config]}" "$2" | cat
	return ${PIPESTATUS[0]}
}

lizardfs_master_daemon() {
	mfsmaster -c "${lizardfs_info_[master${lizardfs_info_[current_master]}_cfg]}" "$1" | cat
	return ${PIPESTATUS[0]}
}

# lizardfs_master_daemon start|stop|restart|kill|tests|isalive|...
lizardfs_master_n() {
	mfsmaster -c "${lizardfs_info_[master${1}_cfg]}" "$2" | cat
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

# Creates MAGIC_DEBUG_LOG which will cause test to fail is some error is logged by any daemon
create_magic_debug_log_entry_() {
	local servername=$1

	# By default, fail on all prefixes passed in DEBUG_LOG_FAIL_ON
	local prefixes=${DEBUG_LOG_FAIL_ON:-}

	# This is a list of other entries, which are added to each test (but can be disabled)
	# Add all these entries to the 'prefixes' list if not disabled using DEBUG_LOG_DISABLE_FAIL_ON
	local auto_prefixes=("fatal.assert" "fatal.abort" "master.mismatch")
	local disable_regex=${DEBUG_LOG_DISABLE_FAIL_ON:-$^} # default value matches nothing
	local prefix
	for prefix in "${auto_prefixes[@]}"; do
		if ! [[ $prefix =~ $disable_regex ]]; then
			prefixes+=" $prefix"
		fi
	done

	# Create MAGIC_DEBUG_LOG_C config entry from all requested prefixes
	if [[ $prefixes ]]; then
		echo -n "MAGIC_DEBUG_LOG_C = "
		for prefix in $prefixes; do
			echo -n "$prefix:$ERROR_DIR/debug_log_errors_${servername}.log,"
		done
		echo
	fi | sed -e 's/,$//'
}

create_mfsmaster_master_cfg_() {
	local this_module_cfg_variable="MASTER_${masterserver_id}_EXTRA_CONFIG"
	echo "PERSONALITY = master"
	echo "SYSLOG_IDENT = master_${masterserver_id}"
	echo "WORKING_USER = $(id -nu)"
	echo "WORKING_GROUP = $(id -ng)"
	echo "EXPORTS_FILENAME = ${lizardfs_info_[master_exports]}"
	if [[ ${lizardfs_info_[master_custom_goals]:-} ]]; then
		echo "CUSTOM_GOALS_FILENAME = ${lizardfs_info_[master_custom_goals]}"
	fi
	echo "DATA_PATH = $masterserver_data_path"
	echo "MATOML_LISTEN_PORT = ${lizardfs_info_[matoml]}"
	echo "MATOCS_LISTEN_PORT = ${lizardfs_info_[matocs]}"
	echo "MATOCL_LISTEN_PORT = ${lizardfs_info_[matocl]}"
	echo "METADATA_CHECKSUM_INTERVAL = 1"
	create_magic_debug_log_entry_ "master_${masterserver_id}"
	echo "${MASTER_EXTRA_CONFIG-}" | tr '|' '\n'
	echo "${!this_module_cfg_variable-}" | tr '|' '\n'
}

create_mfsmaster_shadow_cfg_() {
	local this_module_cfg_variable="MASTER_${masterserver_id}_EXTRA_CONFIG"
	echo "PERSONALITY = shadow"
	echo "SYSLOG_IDENT = shadow_${masterserver_id}"
	echo "WORKING_USER = $(id -nu)"
	echo "WORKING_GROUP = $(id -ng)"
	echo "EXPORTS_FILENAME = ${lizardfs_info_[master_exports]}"
	if [[ ${lizardfs_info_[master_custom_goals]:-} ]]; then
		echo "CUSTOM_GOALS_FILENAME = ${lizardfs_info_[master_custom_goals]}"
	fi
	echo "DATA_PATH = $masterserver_data_path"
	echo "MATOML_LISTEN_PORT = $masterserver_matoml_port"
	echo "MATOCS_LISTEN_PORT = $masterserver_matocs_port"
	echo "MATOCL_LISTEN_PORT = $masterserver_matocl_port"
	echo "MASTER_HOST = $(get_ip_addr)"
	echo "MASTER_PORT = ${lizardfs_info_[matoml]}"
	echo "METADATA_CHECKSUM_INTERVAL = 1"
	create_magic_debug_log_entry_ "shadow_${masterserver_id}"
	echo "${MASTER_EXTRA_CONFIG-}" | tr '|' '\n'
	echo "${!this_module_cfg_variable-}" | tr '|' '\n'
}

lizardfs_make_conf_for_shadow() {
	local target=$1
	cp -f "${lizardfs_info_[master${target}_shadow_cfg]}" "${lizardfs_info_[master${target}_cfg]}"
}

lizardfs_make_conf_for_master() {
	local new_master=$1
	local old_master=${lizardfs_info_[current_master]}
	# move master responsiblity to new masterserver
	cp -f "${lizardfs_info_[master${new_master}_master_cfg]}" "${lizardfs_info_[master${new_master}_cfg]}"
	lizardfs_info_[master_cfg]=${lizardfs_info_[master${new_master}_master_cfg]}
	lizardfs_info_[master_data_path]=${lizardfs_info_[master${new_master}_data_path]}
	lizardfs_info_[current_master]=$new_master
}

lizardfs_current_master_id() {
	echo ${lizardfs_info_[current_master]}
}

prepare_common_metadata_server_files_() {
	create_mfsexports_cfg_ > "$etcdir/mfsexports.cfg"
	lizardfs_info_[master_exports]="$etcdir/mfsexports.cfg"
	if [[ ${MASTER_CUSTOM_GOALS:-} ]]; then
		echo "$MASTER_CUSTOM_GOALS" | tr '|' '\n' > "$etcdir/goals.cfg"
		lizardfs_info_[master_custom_goals]="$etcdir/goals.cfg"
	fi
	get_next_port_number "lizardfs_info_[matoml]"
	get_next_port_number "lizardfs_info_[matocl]"
	get_next_port_number "lizardfs_info_[matocs]"
}

add_metadata_server_() {
	local masterserver_id=$1
	local personality=$2

	local masterserver_matoml_port
	local masterserver_matocl_port
	local masterserver_matocs_port
	local masterserver_data_path=$vardir/master${masterserver_id}
	local masterserver_master_cfg=$etcdir/mfsmaster${masterserver_id}_master.cfg
	local masterserver_shadow_cfg=$etcdir/mfsmaster${masterserver_id}_shadow.cfg
	local masterserver_cfg=$etcdir/mfsmaster${masterserver_id}.cfg

	get_next_port_number masterserver_matoml_port
	get_next_port_number masterserver_matocl_port
	get_next_port_number masterserver_matocs_port
	mkdir "$masterserver_data_path"
	create_mfsmaster_master_cfg_ > "$masterserver_master_cfg"
	create_mfsmaster_shadow_cfg_ > "$masterserver_shadow_cfg"

	if [[ "$personality" == "master" ]]; then
		cp "$masterserver_master_cfg" "$masterserver_cfg"
		echo -n 'MFSM NEW' > "$masterserver_data_path/metadata.mfs"
	elif [[ "$personality" == "shadow" ]]; then
		cp "$masterserver_shadow_cfg" "$masterserver_cfg"
	else
		test_fail "Wrong personality $personality"
	fi

	lizardfs_info_[master${masterserver_id}_shadow_cfg]=$masterserver_shadow_cfg
	lizardfs_info_[master${masterserver_id}_master_cfg]=$masterserver_master_cfg
	lizardfs_info_[master${masterserver_id}_cfg]=$masterserver_cfg
	lizardfs_info_[master${masterserver_id}_data_path]=$masterserver_data_path
	lizardfs_info_[master${masterserver_id}_matoml]=$masterserver_matoml_port
	lizardfs_info_[master${masterserver_id}_matocl]=$masterserver_matocl_port
	lizardfs_info_[master${masterserver_id}_matocs]=$masterserver_matocs_port
}

create_mfsmetalogger_cfg_() {
	echo "WORKING_USER = $(id -nu)"
	echo "WORKING_GROUP = $(id -ng)"
	echo "DATA_PATH = ${lizardfs_info_[master_data_path]}"
	echo "MASTER_HOST = $(get_ip_addr)"
	echo "MASTER_PORT = ${lizardfs_info_[matoml]}"
	create_magic_debug_log_entry_ "mfsmetalogger"
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

# Creates LABEL entry for chunkserver's config from CHUNKSERVER_LABELS variable which is in a form:
# 0,1,2,3:hdd|4,5,6,7:ssd
# Usage: create_chunkserver_label_entry_ <chunkserver_id>
create_chunkserver_label_entry_() {
	local csid=$1
	tr '|' "\n" <<< "${CHUNKSERVER_LABELS-}" | awk -F: '$1~/(^|,)'$csid'(,|$)/ {print "LABEL = "$2}'
}

create_mfschunkserver_cfg_() {
	local this_module_cfg_variable="CHUNKSERVER_${chunkserver_id}_EXTRA_CONFIG"
	echo "SYSLOG_IDENT = chunkserver_${chunkserver_id}"
	echo "WORKING_USER = $(id -nu)"
	echo "WORKING_GROUP = $(id -ng)"
	echo "DATA_PATH = $chunkserver_data_path"
	echo "HDD_CONF_FILENAME = $hdd_cfg"
	echo "MASTER_HOST = $ip_address"
	echo "MASTER_PORT = ${lizardfs_info_[matocs]}"
	echo "CSSERV_LISTEN_PORT = $csserv_port"
	create_chunkserver_label_entry_ "${chunkserver_id}"
	create_magic_debug_log_entry_ "chunkserver_${chunkserver_id}"
	echo "${CHUNKSERVER_EXTRA_CONFIG-}" | tr '|' '\n'
	echo "${!this_module_cfg_variable-}" | tr '|' '\n'
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
	local call="${command_prefix} mfsmount -c ${mount_cfg} ${mount_dir} ${fuse_options}"
	lizardfs_info_[mntcall$mount_id]=$call
	do_mount_ ${mount_id}
}

add_cgi_server_() {
	local cgi_server_port
	local cgi_server_path=$vardir/cgi
	mkdir $cgi_server_path
	get_next_port_number cgi_server_port
	mfscgiserv -D "$cgi_server_path" -P "$cgi_server_port"
	lizardfs_info_[cgi_port]=$cgi_server_port
	lizardfs_info_[cgi_url]="http://localhost:$cgi_server_port/mfs.cgi?masterport=${lizardfs_info_[matocl]}"
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
	local hdds=$(sed -e 's|$|/[A-F0-9][A-F0-9]/|' \
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
		local hdds=$(sed -e 's|$|/[A-F0-9][A-F0-9]/|' "${lizardfs_info_[chunkserver${chunkserver}_hdd]}")
		if (( $# > 0 )); then
			find $hdds -name "chunk*.mfs" -a "(" "$@" ")"
		else
			find $hdds -name "chunk*.mfs"
		fi
	done
}

# A useful shortcut for lizardfs-probe
# Usage: lizardfs_probe_master <command> [option...]
# Calls lizardfs-probe with the given command and and automatically adds address
# of the master server
lizardfs_probe_master() {
	local command="$1"
	shift
	lizardfs-probe "$command" localhost "${lizardfs_info_[matocl]}" --porcelain "$@"
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

# lizardfs_shadow_synchronized <num> -- check if shadow <num> is fully synchronized with master
lizardfs_shadow_synchronized() {
	local num=$1
	local port1=${lizardfs_info_[matocl]}
	local port2=${lizardfs_info_[master${num}_matocl]}
	local probe1="lizardfs-probe metadataserver-status --porcelain localhost $port1"
	local probe2="lizardfs-probe metadataserver-status --porcelain localhost $port2"
	if [[ "$($probe1 | cut -f3)" == "$($probe2 | cut -f3)" ]]; then
		return 0
	else
		return 1
	fi
}

# Prints number of chunks on each chunkserver in the following form:
# <ip1>:<port1> <chunks1>
# <ip2>:<port2> <chunks2>
# ...
lizardfs_rebalancing_status() {
	lizardfs_probe_master list-chunkservers | sort | awk '$2 == "'$LIZARDFS_VERSION'" {print $1,$3}'
}

LIZARDFS_BLOCK_SIZE=$((64 * 1024))
LIZARDFS_CHUNK_SIZE=$((1024 * LIZARDFS_BLOCK_SIZE))
