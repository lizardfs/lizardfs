timeout_set 2 minutes
assert_program_installed attr

master_cfg="MAGIC_DISABLE_METADATA_DUMPS = 1"
master_cfg+="|METADATA_CHECKSUM_RECALCULATION_SPEED = 1"
master_cfg+="|MAGIC_DEBUG_LOG = $TEMP_DIR/log|LOG_FLUSH_ON=DEBUG"

CHUNKSERVERS=1 \
	USE_RAMDISK="YES" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	DEBUG_LOG_FAIL_ON="master.fs.checksum.mismatch" \
	setup_local_empty_lizardfs info

# Create many chunks and directories to work on
count=4000
cd "${info[mount0]}"
for ((fst=0; fst < count; fst++)); do
	echo 1 > chunk_$fst
done
to_create="$(eval echo dir_{0..$count})"
mkdir $to_create
truncate -s 0 "$TEMP_DIR/log"

# Intensively change nodes, edges, chunks and xattrs metadata in background
(
	i=1
	s=$((count / 20))
	while ! test_frozen ; do
		k=$((i * 8009 % count))
		assert_success mv dir_$k a_$k
		assert_success mv a_$k dir_$k
		assert_success attr -qs name -V $i dir_$k
		assert_success attr -qs name -V $i dir_$((k+1))
		for k in {0..19}; do
			assert_success lizardfs setgoal $((1 + i % 7)) chunk_$(((k * s + (i % s)) % count))
		done
		: $((++i))
	done &>/dev/null &
)

# Recalculate metadata checksum in tight loop in background
(
	while ! test_frozen; do
		assert_success lizardfs_admin_master magic-recalculate-metadata-checksum --timeout $(timeout_rescale_seconds 5)
	done &>/dev/null &
)

# Wait for 4 different types of objects to be changed while being recalculated / not recalculated
# This test is non-deterministic and below sometimes fails. If that is the case it should be rerun.
# Really important check is that debug log doesn't contain master.fs.checksum.mismatch.
touch "$TEMP_DIR/log"
expect_eventually_prints 6 'grep -o "changing.*" "$TEMP_DIR/log" | sort | uniq -c | wc -l' "40 seconds"
# Tell which objects were seen and which weren't
log=$(cat "$TEMP_DIR/log" | sort | uniq -c)
for object in {recalculated,not_recalculated}_{node,xattr,chunk}; do
	expect_awk_finds "/master.fs.checksum.changing_$object/" "$log"
done

test_freeze_result # Make it easier to terminate all processes by stopping our loops

