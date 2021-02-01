timeout_set 1000 minutes

# We create file in some custom goal, e.g. '2', or ec21, or ec32.
# We close $CHUNKSERVERS_DOWN_N chunkservers, so that this file is in undergoal,
# and only minimal number of chunkservers for files to be readable remains on.
# (e.g. we close 2 chunkservers when goal=ec32, or 1 when goal=ec21/2).
# We overwrite the file, then restart and upgrade all LizardFS services.
# New version should then remain, and be replicated to previously closed chunkservers.

export LZFS_MOUNT_COMMAND="mfsmount"

source test_utils/upgrade.sh

CHUNKSERVERS=3
	START_WITH_LEGACY_LIZARDFS=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT = $MOOSEFS_CHUNK_FORMAT" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1|OPERATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

REPLICATION_TIMEOUT='3 minutes'
N_10M=20
N_100M=5
N_1G=1
N_2G=1

# chunks number
CN_10M=1
CN_100M=2
CN_1G=16
CN_2G=32


# Start the test with master, two chunkservers and mount running old LizardFS code
# Ensure that we work on legacy version

assert_lizardfsXX_services_count_equals 1 1 $CHUNKSERVERS_N

cd "${info[mount0]}"
mkdir dir
assert_success lizardfsXX mfssetgoal $GOAL dir
cd dir

# map file_size -> cnt, e.g. 20M -> 5 (5 files of file_size 20M)
declare -A file_sizes
fill_array_default_file_sizes file_sizes # always that, or add some custom values here by yourself
assert_success generate_files_various_filesizes file_sizes
echo "Generated, sleeping."
sleep 10m
exit

#assert_success generate_all_files 0
#echo "All files generated"

# Wait until chunk has been replicated
assert_success check_all_files_replicated
echo "All files replicated (3 parts of each chunk)"
# assert_eventually '[[ $(lizardfs checkfile file0 | grep "chunks with 2 copies" | wc -l) == 1 ]]' "$REPLICATION_TIMEOUT"

# We stop 1 chunkserver, and overwrite files
lizardfsXX_chunkserver_daemon 0 stop
echo "Chunkserver 0 stopped."

echo "Overwriting half files."
assert_success overwrite_half_files

echo "Validating all files."
assert_success validate_all_files
echo "All files validated."
# assert_equals $new_file_content $(cat file0)

echo "Listing mounts."
lizardfs_admin_master list-mounts

# Restart all services, start in new version
cd "$TEMP_DIR"
lizardfsXX_chunkserver_daemon 1 stop
lizardfsXX_chunkserver_daemon 2 stop
assert_success lizardfs_mount_unmount 0
sleep 10
lizardfs_admin_master list-mounts
lizardfs_master_daemon restart
lizardfs_mount_start 0
lizardfs_chunkserver_daemon 0 start
lizardfs_chunkserver_daemon 1 start
lizardfs_chunkserver_daemon 2 start

sleep 10

echo "Listing mounts (to be sure no 3.12 still prevails)"
lizardfs_admin_master list-mounts
echo "Grepped"
# lizardfs_admin_master list-mounts | grep $LIZARDFSXX_TAG
echo "Grepped | wc -l"
lizardfs_admin_master list-mounts | grep $LIZARDFSXX_TAG | wc -l

# Ensure that versions are switched
assert_equals 0 $(lizardfs_admin_master info | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 0 $(lizardfs_admin_master list-chunkservers | grep $LIZARDFSXX_TAG | wc -l)
assert_equals 0 $(lizardfs_admin_master list-mounts | grep $LIZARDFSXX_TAG | wc -l)

echo "3.13 is On"
# Check if new content was preserved, not old one. On both chunkservers.
cd "${info[mount0]}/dir"
assert_success check_all_files_replicated

echo "ALL REPLICATED"

# assert_eventually '[[ $(lizardfs checkfile file0 | grep "chunks with 2 copies" | wc -l) == 1 ]]' "$REPLICATION_TIMEOUT"

echo "Validating files with 3/3 CS on"
assert_success validate_all_files
# assert_equals $new_file_content $(cat file0)

echo "Validating files with CS 1 2 on"
lizardfs_chunkserver_daemon 0 stop
sleep 5 # TODO wait_for_chunkservers_started czy coś takiego
assert_success validate_all_files
# assert_equals $new_file_content $(cat file0)

echo "Validating files with CS 0 2 on"
lizardfs_chunkserver_daemon 1 stop
lizardfs_chunkserver_daemon 0 start
sleep 5 # TODO wait_for_chunkservers_started czy coś takiego
assert_success validate_all_files
# assert_equals $new_file_content $(cat file0)

echo "Validating files with CS 0 1 on"
lizardfs_chunkserver_daemon 2 stop
lizardfs_chunkserver_daemon 1 start
sleep 5 # TODO wait_for_chunkservers_started czy coś takiego
assert_success validate_all_files

sleep 25m
assert_success validate_all_files

function generate_file {
	local name=$1
	local seed=$2
	local size=$3
	echo "Generating $name"
	FILE_SIZE=$size SEED=$seed file-generate $name
}

function generate_one_dir {
	local dirname=$1
	local n_files=$2
	local seed_shift=$3
	cd $dirname
	echo "Generating files in directory: $dirname"
	for i in $(seq $n_files); do
		generate_file $i $((i + seed_shift)) $dirname
	done
	cd -
}

function generate_all_files {
	local seed_shift=$1
	mkdir 10M 100M 1G 2G
	generate_one_dir 10M $N_10M $seed_shift
	generate_one_dir 100M $N_100M $seed_shift
	generate_one_dir 1G $N_1G $seed_shift
	generate_one_dir 2G $N_2G $seed_shift
}

function overwrite_half_files {
	local seed_shift=1000
	generate_one_dir 10M $(((N_10M + 1) / 2)) $seed_shift
	generate_one_dir 100M $(((N_100M + 1) / 2)) $seed_shift
	generate_one_dir 1G $(((N_1G + 1) / 2)) $seed_shift
	generate_one_dir 2G $(((N_2G + 1) / 2)) $seed_shift
}

function validate_one_dir {
	local dirname=$1
	local n_files=$2
	local n_overwritten=$(((n_files + 1) / 2))
	cd $dirname
	for i in $(seq 1 $n_overwritten); do
		SEED=$((1000 + $i)) assert_success file-validate $i
	done
	# normal, old files, seed=default
	for i in $(seq $((n_overwritten + 1)) n_files); do
		SEED=$i assert_success file-validate $i
	done
	cd -
}

function validate_all_files {
	validate_one_dir 10M $N_10M
	validate_one_dir 100M $N_100M
	validate_one_dir 1G $N_1G
	validate_one_dir 2G $N_2G
}

function check_one_dir_replicated {
	local dirname=$1
	local n_chunks=$2
	cd $dirname
	for f in *; do
		[ -f $f ] || continue
		echo $dirname $f
		echo "Checking if replicated properly, dir: $dirname, file: $f"
		lizardfs checkfile $f
		lizardfs fileinfo $f
		assert_eventually '[[ $(lizardfs fileinfo $f | grep "copy" | wc -l) == $(( n_chunks * 3 )) ]]' "$REPLICATION_TIMEOUT"
	done
	cd -
}

function check_all_files_replicated {
	check_one_dir_replicated 10M $CN_10M
	check_one_dir_replicated 100M $CN_100M
	check_one_dir_replicated 1G $CN_1G
	check_one_dir_replicated 2G $CN_2G
}
