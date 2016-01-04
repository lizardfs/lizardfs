timeout_set 10 minutes

CHUNKSERVERS=8 \
	DISK_PER_CHUNKSERVER=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_CUSTOM_GOALS="6 ec: \$ec(5,3)" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

dir="${info[mount0]}/dir"
mkdir "$dir"
lizardfs setgoal ec "$dir"
FILE_SIZE=876M file-generate "$dir/file"

for i in {0..7}; do
	for cs in {0..2}; do
		mfschunkserver -c "${info[chunkserver$(((i + cs) % 8))_config]}" stop
	done
	if ! file-validate "$dir/file"; then
		test_add_failure "Data read from file without chunkservers $i-$(((i + cs) % 8)) is different than written"
	fi
	for cs in {0..2}; do
		mfschunkserver -c "${info[chunkserver$(((i + cs) % 8))_config]}" start
	done
	lizardfs_wait_for_all_ready_chunkservers
done
