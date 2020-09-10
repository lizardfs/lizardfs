valgrind_enable

number_of_mounts=2

CHUNKSERVERS=1 \
	MOUNTS=${number_of_mounts} \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

mnt0dir1="${info[mount0]}/dir1"
mnt0dir2="${info[mount0]}/dir2"
mnt1dir2="${info[mount1]}/dir2"
mkdir "$mnt0dir1"

head -c 8765432 /dev/urandom > "$mnt0dir1/file"
mv "$mnt0dir1/file" "$mnt0dir1/file2"
ls -l "$mnt0dir1" > /dev/null
truncate "$mnt0dir1/file2" -s 9876543
truncate "$mnt0dir1/file2" -s 8M

lizardfs settrashtime 0 "$mnt0dir1"
lizardfs makesnapshot "$mnt0dir1" "$mnt0dir2"
dd if=/dev/zero of="$mnt0dir2/file2" bs=1 seek=1M count=10 conv=notrunc

FILE_SIZE=8M file-generate "$RAMDISK_DIR/file"
dd if="$RAMDISK_DIR/file" of="$mnt1dir2/file2" bs=1k skip=0 seek=0 count=4k conv=notrunc &
dd if="$RAMDISK_DIR/file" of="$mnt0dir2/file2" bs=1k skip=4k seek=4k count=4k conv=notrunc &
wait

MESSAGE="Validating $mnt1dir2/file2" expect_success file-validate "$mnt1dir2/file2" &
MESSAGE="Validating $mnt0dir2/file2" expect_success file-validate "$mnt0dir2/file2" &
wait

rm "$mnt0dir1/file2"

lizardfs_master_daemon restart
lizardfs_chunkserver_daemon 0 restart
lizardfs_wait_for_all_ready_chunkservers

# Valgrind does not unmount the mounts correctly
# so we need to do it here
# to avoid false positive detections
for ((mntid=0 ; mntid<number_of_mounts; ++mntid)); do
	lizardfs_mount_unmount $mntid
done

# Here we need to wait for unmount asynchronic tasks
# the long timeout is set to detect deadlocks
wait_for '! pgrep -f -u lizardfstest mfsmount >/dev/null' '30 minutes' || true
