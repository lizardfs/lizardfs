timeout_set 10 minutes

CHUNKSERVERS=10 \
	DISK_PER_CHUNKSERVER=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

dir="${info[mount0]}/dir"
mkdir "$dir"
mfssetgoal xor9 "$dir"
FILE_SIZE=876M file-generate "$dir/file"

for i in {0..9}; do
	mfschunkserver -c "${info[chunkserver${i}_config]}" stop
	if ! file-validate "$dir/file"; then
		test_add_failure "Data read from file without chunkserver $i is different than written"
	fi
	mfschunkserver -c "${info[chunkserver${i}_config]}" start
	sleep 2
done