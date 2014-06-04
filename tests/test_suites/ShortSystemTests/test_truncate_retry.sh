timeout_set '45 seconds'

# 6 retries means the truncate can wait up to 12.6s (0.2 + 0.4 + 0.8 + 1.6 + 3.2 + 6.4)
CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER|mfsioretries=6" \
	setup_local_empty_lizardfs info

cd ${info[mount0]}

FILE_SIZE=1234 file-generate file
# file-validate would expect a 1234 byte file, so validation is performed using md5sum
checksum123="$(head -c 123 file | md5sum)"

lizardfs_chunkserver_daemon 0 stop
assert_awk_finds '/no valid copies/' "$(mfsfileinfo file)"
# Following operation should pass - wake up just after 4th try (0.2 + 0.4 + 0.8 + 1.6 = 3.0s)
(sleep 3.1 && lizardfs_chunkserver_daemon 0 start) & truncate -s 123 file
assert_awk_finds '/[0-9A-F]+_00000002/' "$(mfsfileinfo file)"
assert_equals "$checksum123" "$(cat file | md5sum)"

lizardfs_chunkserver_daemon 0 stop
assert_awk_finds '/no valid copies/' "$(mfsfileinfo file)"
# Following operation shouldn't pass - waiting time's exceeded
sleep 12.7 && lizardfs_chunkserver_daemon 0 start &
assert_failure truncate -s 12 file

# Second truncate should produce chunk in version 3 - check if it isn't created
lizardfs_wait_for_ready_chunkservers 1
assert_awk_finds_no '/[0-9A-F]+_00000003/' "$(mfsfileinfo file)"
# Check if file is OK even if truncate was failed
assert_equals "$checksum123" "$(cat file | md5sum)"
