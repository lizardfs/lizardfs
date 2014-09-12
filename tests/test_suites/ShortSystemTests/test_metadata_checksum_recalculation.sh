timeout_set 1 minute
assert_program_installed attr

master_cfg="MAGIC_DISABLE_METADATA_DUMPS = 1"
master_cfg+="|RECALCULATE_CHECKSUM_ON_RELOAD = 1"
master_cfg+="|METADATA_CHECKSUM_RECALCULATION_SPEED = 1"
master_cfg+="|MAGIC_DEBUG_LOG = master.fs.checksum:$TEMP_DIR/log,master.fs.checksum.updater_end:$TEMP_DIR/ends"

CHUNKSERVERS=1 \
	USE_RAMDISK="YES" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="$master_cfg" \
	DEBUG_LOG_FAIL_ON="master.fs.checksum.mismatch" \
	setup_local_empty_lizardfs info

# Create many chunks and directories to work on
cd "${info[mount0]}"
echo 1 | tee chunk_{0..500} >/dev/null
mkdir dir_{0..1000}
truncate -s 0 "$TEMP_DIR/log"

# Intensively change nodes, edges, chunks and xattrs metadata in background
(
	i=1
	while ! test_frozen ; do
		k=$((i * 8009 % 1000))
		assert_success mv dir_$k a_$k
		assert_success mv a_$k dir_$k
		assert_success attr -qs name -V $i dir_$k
		assert_success attr -qs name -V $i dir_$((k+1))
		for k in {1..20}; do
			assert_success mfssetgoal $((1 + i % 7)) chunk_$(((i * k) % 500))
		done
		: $((++i))
	done &>/dev/null &
)

# Recalculate metadata checksum in tight loop in background
(
	while ! test_frozen; do
		truncate -s 0 "$TEMP_DIR/ends"
		lizardfs_master_daemon reload;
		assert_eventually 'grep -q updater_end $TEMP_DIR/ends'
	done &>/dev/null &
)

# Wait for 4 different types of objects to be changed while being recalculated / not recalculated
touch "$TEMP_DIR/log"
if valgrind_enabled; then
	timeout="5 minutes"
else
	timeout="40 seconds"
fi
expect_eventually_prints 8 'grep -o "changing.*" "$TEMP_DIR/log" | sort -u | wc -l' "$timeout"

# Tell which objects were seen and which weren't
log=$(cat "$TEMP_DIR/log" | sort | uniq -c)
for object in {recalculated,not_recalculated}_{edge,node,xattr,chunk}; do
	expect_awk_finds "/master.fs.checksum.changing_$object/" "$log"
done

test_freeze_result # Make it easier to termiate all processes by stopping our loops

