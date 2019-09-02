# Usage: setup_local_empty_lizardfs out_var
# Configures and starts master, chunkserver and mounts
# If out_var provided an associative array with name $out_var
# is created and it contains information about the filestystem
setup_local_empty_lizardfs() {
	local use_moosefs=${USE_MOOSEFS:-}
	local use_lizardfsXX=${LIZARDFSXX_TAG:-}
	local use_ramdisk=${USE_RAMDISK:-}
	local use_loop=${USE_LOOP_DISKS:-}
	local number_of_masterservers=${MASTERSERVERS:-1}
	local number_of_chunkservers=${CHUNKSERVERS:-1}
	local number_of_mounts=${MOUNTS:-1}
	local disks_per_chunkserver=${DISK_PER_CHUNKSERVER:-1}
	local auto_shadow_master=${AUTO_SHADOW_MASTER:-YES}
	local cgi_server=${CGI_SERVER:-NO}
	local ip_address=$(get_ip_addr)
	local etcdir=$TEMP_DIR/lizardfs/etc
	local vardir=$TEMP_DIR/lizardfs/var
	local mntdir=$TEMP_DIR/mnt
	local master_start_param=${MASTER_START_PARAM:-}
	local shadow_start_param=${SHADOW_START_PARAM:-}
	declare -gA lizardfs_info_
	lizardfs_info_[chunkserver_count]=$number_of_chunkservers
	lizardfs_info_[admin_password]=${ADMIN_PASSWORD:-password}

	# Try to enable core dumps if possible
	if [[ $(ulimit -c) == 0 ]]; then
		ulimit -c unlimited || ulimit -c 100000000 || ulimit -c 1000000 || ulimit -c 10000 || :
	fi

	# Prepare directories for LizardFS
	mkdir -p "$etcdir" "$vardir"

	use_new_goal_config="true"
	local oldpath="$PATH"
	if [[ $use_moosefs ]]; then
		use_new_goal_config="false"
		export PATH="$MOOSEFS_DIR/bin:$MOOSEFS_DIR/sbin:$PATH"
		build_moosefs
	fi

	if [[ $use_lizardfsXX ]]; then
		if version_compare_gte "$LIZARDFSXX_TAG" "3.11.0" ; then
			use_new_goal_config="true"
		else
			use_new_goal_config="false"
		fi
		LIZARDFSXX_DIR=${LIZARDFSXX_DIR_BASE}/lizardfs-${LIZARDFSXX_TAG}
		export PATH="${LIZARDFSXX_DIR}/bin:${LIZARDFSXX_DIR}/sbin:$PATH"
		build_lizardfsXX
	fi

	# Prepare configuration for metadata servers
	use_new_format=$use_new_goal_config prepare_common_metadata_server_files_
	add_metadata_server_ 0 "master"
	for ((msid=1 ; msid<number_of_masterservers; ++msid)); do
		add_metadata_server_ $msid "shadow"
	done
	lizardfs_info_[current_master]=0
	lizardfs_info_[master_cfg]=${lizardfs_info_[master0_cfg]}
	lizardfs_info_[master_data_path]=${lizardfs_info_[master0_data_path]}
	lizardfs_info_[masterserver_count]=$number_of_masterservers

	# Start one masterserver with personality master
	lizardfs_master_daemon start ${master_start_param}

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
		lizardfs_master_n auto start ${shadow_start_param}
		assert_eventually 'lizardfs_shadow_synchronized auto'
	fi

	if [[ $cgi_server == YES ]]; then
		add_cgi_server_
	fi

	# Wait for chunkservers (use lizardfs-probe only for LizardFS -- MooseFS doesn't support it)
	if [[ ! $use_moosefs ]]; then
		lizardfs_wait_for_all_ready_chunkservers
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

lizardfs_fusermount() {
	fuse_version=$(${LZFS_MOUNT_COMMAND} --version 2>&1 | grep "FUSE library" | grep -Eo "[0-9]+\..+")
	if [[ "$fuse_version" =~ ^3\..+$ ]]; then
		fusermount3 "$@"
	else
		fusermount "$@"
	fi
}

# lizardfs_chunkserver_daemon <id> start|stop|restart|kill|tests|isalive|...
lizardfs_chunkserver_daemon() {
	local id=$1
	shift
	mfschunkserver -c "${lizardfs_info_[chunkserver${id}_cfg]}" "$@" | cat
	return ${PIPESTATUS[0]}
}

lizardfs_master_daemon() {
	mfsmaster -c "${lizardfs_info_[master${lizardfs_info_[current_master]}_cfg]}" "$@" | cat
	return ${PIPESTATUS[0]}
}

# lizardfs_master_daemon start|stop|restart|kill|tests|isalive|...
lizardfs_master_n() {
	local id=$1
	shift
	mfsmaster -c "${lizardfs_info_[master${id}_cfg]}" "$@" | cat
	return ${PIPESTATUS[0]}
}

# lizardfs_metalogger_daemon start|stop|restart|kill|tests|isalive|...
lizardfs_metalogger_daemon() {
	mfsmetalogger -c "${lizardfs_info_[metalogger_cfg]}" "$@" | cat
	return ${PIPESTATUS[0]}
}

# lizardfs_mount_unmount <id>
lizardfs_mount_unmount() {
	local mount_id=$1
	local mount_dir=${lizardfs_info_[mount${mount_id}]}
	lizardfs_fusermount -u ${mount_dir}
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

create_mfsgoals_cfg_() {
	local goal_name

	for i in {1..5}; do
		echo "${MASTER_CUSTOM_GOALS:-}" | tr '|' '\n' | grep "^$i " | cat
	done
	for i in {6..20}; do
		wildcards=
		for ((j=0; j<i; j++)); do
			wildcards="$wildcards _"
		done;
		(echo "${MASTER_CUSTOM_GOALS:-}" | tr '|' '\n' | grep "^$i ") || echo "$i $i: $wildcards"
	done;
	if [[ $use_new_format == "true" ]]; then
		for i in {2..9}; do
			echo "$((21 + $i)) xor$i: \$xor$i"
		done;
		for i in {2..4}; do
			echo "$((31 + 2*(i - 2) + 0)) ec$i$((i - 1)): \$ec($i,$((i - 1)))"
			echo "$((31 + 2*(i - 2) + 1)) ec$i$i: \$ec($i,$i)"
		done
	fi;
}

create_mfstopology_cfg_() {
	echo '# empty topology...'
}

# Creates MAGIC_DEBUG_LOG which will cause test to fail is some error is logged by any daemon
create_magic_debug_log_entry_() {
	local servername=$1

	# By default, fail on all prefixes passed in DEBUG_LOG_FAIL_ON
	local prefixes=${DEBUG_LOG_FAIL_ON:-}

	local prefix
	# Create MAGIC_DEBUG_LOG_C config entry from all requested prefixes
	if [[ $prefixes ]]; then
		echo -n "MAGIC_DEBUG_LOG_C = "
		for prefix in $prefixes; do
			echo -n "$prefix:$ERROR_DIR/debug_log_errors_${servername}.log,"
		done
		echo
	fi | sed -e 's/,$//'
}

# Sometimes use Berkley DB name storage
create_bdb_name_storage_entry_() {
	if (($RANDOM % 2)); then
		echo "USE_BDB_FOR_NAME_STORAGE = 0"
	else
		echo "USE_BDB_FOR_NAME_STORAGE = 1"
	fi
}

create_mfsmaster_master_cfg_() {
	local this_module_cfg_variable="MASTER_${masterserver_id}_EXTRA_CONFIG"
	echo "PERSONALITY = master"
	echo "SYSLOG_IDENT = master_${masterserver_id}"
	echo "WORKING_USER = $(id -nu)"
	echo "WORKING_GROUP = $(id -ng)"
	echo "EXPORTS_FILENAME = ${lizardfs_info_[master_exports]}"
	echo "TOPOLOGY_FILENAME = ${lizardfs_info_[master_topology]}"
	echo "CUSTOM_GOALS_FILENAME = ${lizardfs_info_[master_custom_goals]}"
	echo "DATA_PATH = $masterserver_data_path"
	echo "MATOML_LISTEN_PORT = ${lizardfs_info_[matoml]}"
	echo "MATOCS_LISTEN_PORT = ${lizardfs_info_[matocs]}"
	echo "MATOCL_LISTEN_PORT = ${lizardfs_info_[matocl]}"
	echo "MATOTS_LISTEN_PORT = ${lizardfs_info_[matots]}"
	echo "METADATA_CHECKSUM_INTERVAL = 1"
	echo "ADMIN_PASSWORD = ${lizardfs_info_[admin_password]}"
	create_magic_debug_log_entry_ "master_${masterserver_id}"
	echo "${MASTER_EXTRA_CONFIG-}" | tr '|' '\n'
	echo "${!this_module_cfg_variable-}" | tr '|' '\n'
	create_bdb_name_storage_entry_
}

create_mfsmaster_shadow_cfg_() {
	local this_module_cfg_variable="MASTER_${masterserver_id}_EXTRA_CONFIG"
	echo "PERSONALITY = shadow"
	echo "SYSLOG_IDENT = shadow_${masterserver_id}"
	echo "WORKING_USER = $(id -nu)"
	echo "WORKING_GROUP = $(id -ng)"
	echo "EXPORTS_FILENAME = ${lizardfs_info_[master_exports]}"
	echo "TOPOLOGY_FILENAME = ${lizardfs_info_[master_topology]}"
	echo "CUSTOM_GOALS_FILENAME = ${lizardfs_info_[master_custom_goals]}"
	echo "DATA_PATH = $masterserver_data_path"
	echo "MATOML_LISTEN_PORT = $masterserver_matoml_port"
	echo "MATOCS_LISTEN_PORT = $masterserver_matocs_port"
	echo "MATOCL_LISTEN_PORT = $masterserver_matocl_port"
	echo "MATOTS_LISTEN_PORT = $masterserver_matots_port"
	echo "MASTER_HOST = $(get_ip_addr)"
	echo "MASTER_PORT = ${lizardfs_info_[matoml]}"
	echo "METADATA_CHECKSUM_INTERVAL = 1"
	echo "ADMIN_PASSWORD = ${lizardfs_info_[admin_password]}"
	create_magic_debug_log_entry_ "shadow_${masterserver_id}"
	echo "${MASTER_EXTRA_CONFIG-}" | tr '|' '\n'
	echo "${!this_module_cfg_variable-}" | tr '|' '\n'
	create_bdb_name_storage_entry_
}

lizardfs_make_conf_for_shadow() {
	local target=$1
	cp -f "${lizardfs_info_[master${target}_shadow_cfg]}" "${lizardfs_info_[master${target}_cfg]}"
}

lizardfs_make_conf_for_master() {
	local new_master=$1
	local old_master=${lizardfs_info_[current_master]}
	# move master responsibility to new masterserver
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
	create_mfstopology_cfg_ > "$etcdir/mfstopology.cfg"
	create_mfsgoals_cfg_ > "$etcdir/mfsgoals.cfg"
	lizardfs_info_[master_exports]="$etcdir/mfsexports.cfg"
	lizardfs_info_[master_topology]="$etcdir/mfstopology.cfg"
	lizardfs_info_[master_custom_goals]="$etcdir/mfsgoals.cfg"
	get_next_port_number "lizardfs_info_[matoml]"
	get_next_port_number "lizardfs_info_[matocl]"
	get_next_port_number "lizardfs_info_[matocs]"
	get_next_port_number "lizardfs_info_[matots]"
}

add_metadata_server_() {
	local masterserver_id=$1
	local personality=$2

	local masterserver_matoml_port
	local masterserver_matocl_port
	local masterserver_matocs_port
	local masterserver_matots_port
	local masterserver_data_path=$vardir/master${masterserver_id}
	local masterserver_master_cfg=$etcdir/mfsmaster${masterserver_id}_master.cfg
	local masterserver_shadow_cfg=$etcdir/mfsmaster${masterserver_id}_shadow.cfg
	local masterserver_cfg=$etcdir/mfsmaster${masterserver_id}.cfg

	get_next_port_number masterserver_matoml_port
	get_next_port_number masterserver_matocl_port
	get_next_port_number masterserver_matocs_port
	get_next_port_number masterserver_matots_port
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
	lizardfs_info_[master${masterserver_id}_matots]=$masterserver_matocs_port
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
		for (( disk_number=0; disk_number<n; disk_number++ )); do
			# Use path provided in env variable, if present generate some pathname otherwise.
			local this_disk_variable="CHUNKSERVER_${chunkserver_id}_DISK_${disk_number}"
			if [[ ${!this_disk_variable-} ]]; then
				local disk_dir=${!this_disk_variable}
			else
				local disk_dir=$RAMDISK_DIR/hdd_${chunkserver_id}_${disk_number}
			fi
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
	echo "HDD_LEAVE_SPACE_DEFAULT = 128MiB"
	echo "MASTER_HOST = $ip_address"
	echo "MASTER_PORT = ${lizardfs_info_[matocs]}"
	echo "CSSERV_LISTEN_PORT = $csserv_port"
	create_chunkserver_label_entry_ "${chunkserver_id}"
	create_magic_debug_log_entry_ "chunkserver_${chunkserver_id}"
	echo "${CHUNKSERVER_EXTRA_CONFIG-}" | tr '|' '\n'
	echo "${!this_module_cfg_variable-}" | tr '|' '\n'
}

# Run every second chunkserver with each chunk format
chunkserver_chunk_format_cfg_() {
	local chunkserver_id=$1
	if (($RANDOM % 2)); then
		echo "CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = 0"
	else
		echo "CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = 1"
	fi
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
	# If the chunk format wasn't yet chosen set it:
	grep CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT "$chunkserver_cfg" || \
		chunkserver_chunk_format_cfg_ $chunkserver_id >> "$chunkserver_cfg"
	mkdir -p "$chunkserver_data_path"
	mfschunkserver -c "$chunkserver_cfg" start

	lizardfs_info_[chunkserver${chunkserver_id}_port]=$csserv_port
	lizardfs_info_[chunkserver${chunkserver_id}_cfg]=$chunkserver_cfg
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
	local workingdir=$TEMP_DIR/var/mount$mount_id
	# Make sure mount process is in a writable directory (for core dumps)
	mkdir -p $workingdir
	cd $workingdir
	for try in $(seq 1 $max_tries); do
		${lizardfs_info_[mntcall${mount_id}]} && return 0
		echo "Retrying in 1 second ($try/$max_tries)..."
		sleep 1
	done
	echo "Cannot mount in $mount_dir, exiting"
	cd -
	exit 2
}

add_mount_() {
	local mount_id=$1
	local mount_dir=$mntdir/mfs${mount_id}
	local mount_cfg=$etcdir/mfsmount${mount_id}.cfg
	create_mfsmount_cfg_ ${mount_id} > "$mount_cfg"
	mkdir -p "$mount_dir"
	lizardfs_info_[mount${mount_id}]="$mount_dir"
	lizardfs_info_[mount${mount_id}_cfg]="$mount_cfg"
	max_tries=30

	if [ -z ${LZFS_MOUNT_COMMAND+x} ]; then
		if (($RANDOM % 2)); then
			LZFS_MOUNT_COMMAND=mfsmount3
			echo "Using libfuse3 for mounting filesystem."
		else
			LZFS_MOUNT_COMMAND=mfsmount
		fi
	fi

	fuse_options=""
	for fuse_option in $(echo ${FUSE_EXTRA_CONFIG-} | tr '|' '\n'); do
		fuse_option_name=$(echo $fuse_option | cut -f1 -d'=')
		${LZFS_MOUNT_COMMAND} --help |& grep " -o ${fuse_option_name}[ =]" > /dev/null \
				|| test_fail "Your libfuse doesn't support $fuse_option_name flag"
		fuse_options+="-o $fuse_option "
	done
	local call="${command_prefix} ${LZFS_MOUNT_COMMAND} -c ${mount_cfg} ${mount_dir} ${fuse_options}"
	lizardfs_info_[mntcall$mount_id]=$call
	do_mount_ ${mount_id}
}

add_cgi_server_() {
	local cgi_server_port
	local pidfile="$vardir/lizardfs-cgiserver.pid"
	get_next_port_number cgi_server_port
	lizardfs-cgiserver -P "$cgi_server_port" -p "$pidfile"
	lizardfs_info_[cgi_pidfile]=$pidfile
	lizardfs_info_[cgi_port]=$cgi_server_port
	lizardfs_info_[cgi_url]="http://localhost:$cgi_server_port/mfs.cgi?masterport=${lizardfs_info_[matocl]}"
}

# Some helper functions for tests to manipulate the existing installation

mfs_dir_info() {
	if (( $# != 2 )); then
		echo "Incorrect usage of lizardfs dir_info with args $*";
		exit 2;
	fi;
	field=$1
	file=$2
	lizardfs dirinfo "$file" | grep -w "$field" | grep -o '[0-9]*'
}

find_first_chunkserver_with_chunks_matching() {
	local pattern=$1
	local count=${lizardfs_info_[chunkserver_count]}
	local chunkserver
	for (( chunkserver=0 ; chunkserver < count ; ++chunkserver )); do
		local hdds=$(cat "${lizardfs_info_[chunkserver${chunkserver}_hdd]}")
		if [[ $(find $hdds -type f -name "$pattern") ]]; then
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
	local hdds=$(sed -e 's/*//' -e 's|$|/chunks[A-F0-9][A-F0-9]/|' \
			"${lizardfs_info_[chunkserver${chunkserver_number}_hdd]}")
	if (( $# > 0 )); then
		find $hdds "(" -name 'chunk*.liz' -o -name 'chunk*.mfs' ")" -a "(" "$@" ")"
	else
		find $hdds "(" -name 'chunk*.liz' -o -name 'chunk*.mfs' ")"
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

# A useful shortcut for lizardfs-probe
# Usage: lizardfs_probe_master <command> [option...]
# Calls lizardfs-probe with the given command and and automatically adds address
# of the master server
lizardfs_probe_master() {
	local command="$1"
	shift
	lizardfs-probe "$command" localhost "${lizardfs_info_[matocl]}" --porcelain "$@"
}

# A useful shortcut for lizardfs-admin commands which require authentication
# Usage: lizardfs_admin_master <command> [option...]
# Calls lizardfs-admin with the given command and and automatically adds address
# of the master server and authenticates
lizardfs_admin_master() {
	local command="$1"
	shift
	local port=${lizardfs_info_[matocl]}
	lizardfs-admin "$command" localhost "$port" "$@" <<< "${lizardfs_info_[admin_password]}"
}

# A useful shortcut for lizardfs-admin commands which require authentication
# Usage: lizardfs_admin_shadow <n> <command> [option...]
# Calls lizardfs-admin with the given command and and automatically adds address
# of the n'th shadow master server and authenticates
lizardfs_admin_shadow() {
	local id="$1"
	local command="$2"
	shift 2
	local port=${lizardfs_info_[master${id}_matocl]}
	lizardfs-admin "$command" localhost "$port" "$@" <<< "${lizardfs_info_[admin_password]}"
}

# Stops the active master server without dumping metadata
lizardfs_stop_master_without_saving_metadata() {
	lizardfs_admin_master stop-master-without-saving-metadata
	assert_eventually "! mfsmaster -c ${lizardfs_info_[master_cfg]} isalive"
}

# print the number of fully operational chunkservers
lizardfs_ready_chunkservers_count() {
	lizardfs-probe ready-chunkservers-count localhost ${lizardfs_info_[matocl]}
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
# <ip1>:<port1>:<label> <chunks1>
# <ip2>:<port2>:<label> <chunks2>
# ...
lizardfs_rebalancing_status() {
	lizardfs_probe_master list-chunkservers | sort | awk '$2 == "'$LIZARDFS_VERSION'" {print $1":"$10,$3}'
}
