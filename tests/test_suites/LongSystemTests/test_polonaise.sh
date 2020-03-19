timeout_set '30 minutes'

assert_program_installed git
assert_program_installed cmake
assert_program_installed lizardfs-polonaise-server
assert_program_installed polonaise-fuse-client

CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

MINIMUM_PARALLEL_JOBS=4
MAXIMUM_PARALLEL_JOBS=16
PARALLEL_JOBS=$(get_nproc_clamped_between ${MINIMUM_PARALLEL_JOBS} ${MAXIMUM_PARALLEL_JOBS})

# Start Polonaise
lizardfs-polonaise-server \
	--master-host=localhost \
	--master-port=${info[matocl]} \
	--bind-port 9090 &> /dev/null &
sleep 3
mnt="$TEMP_DIR/mfspolon"
mkdir -p "$mnt"
polonaise-fuse-client "$mnt" -o allow_other &
assert_eventually 'lizardfs dirinfo "$mnt"'

# Perform a compilation
cd "$mnt"
assert_success git clone https://github.com/lizardfs/lizardfs.git
mkdir lizardfs/build
cd lizardfs/build
assert_success cmake .. -DCMAKE_INSTALL_PREFIX="$mnt"
make -j${PARALLEL_JOBS} install
