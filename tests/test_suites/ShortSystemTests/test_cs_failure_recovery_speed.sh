CHUNKSERVERS=4 \
	USE_RAMDISK=YES \
	MOUNTS=5 \
	CHUNKSERVER_EXTRA_CONFIG="MASTER_TIMEOUT = 2" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER|mfschunkserverwriteto=500" \
	setup_local_empty_lizardfs info

# Create 5 files containing some garbage
mkdir "${info[mount0]}/dir"
mfssetgoal 4 "${info[mount0]}/dir"
head -c 1M /dev/urandom | tee "${info[mount0]}"/dir/file{0,1,2,3,4} >/dev/null

# Stop one of chunkservers which has one copy of each chunk and
# immediately start overwriting all the chunks.
kill -s SIGSTOP "$(lizardfs_chunkserver_daemon 0 test 2>&1 | sed 's/.*pid: //')"
for i in {0..4}; do
	( assert_success file-overwrite "${info[mount$i]}/dir/file$i" && touch "$TEMP_DIR/finish$i" & )
done

# Expect these tasks to finish not later than after 4 seconds. The timeout is calculated as:
# 2 s (ma<->cs timeout) + 0.5 s (cl<->cs timeout) + 1 s (client can wait ~1 s between retries)
assert_success wait_for '(( $(ls "$TEMP_DIR"/finish? 2>/dev/null | wc -l) == 5 ))' '4 seconds'
assert_success file-validate "${info[mount0]}/dir/"file*
