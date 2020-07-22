# Set up an installation with three server rooms
USE_RAMDISK=YES \
	CHUNKSERVERS=9 \
	CHUNKSERVER_LABELS="0,1,2:sr1|3,4,5:sr2|6,7,8:sr3" \
	MASTER_CUSTOM_GOALS="10 three_serverrooms: sr1 sr2 sr3" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER|ENDANGERED_CHUNKS_PRIORITY=1" \
	setup_local_empty_lizardfs info

# Create many files, each should have at least one copy in each server room
cd "${info[mount0]}"
mkdir "${info[mount0]}/dir"
lizardfs setgoal three_serverrooms "${info[mount0]}/dir"
for size in {1..15}M {50,100,200}M; do
	FILE_SIZE="$size" assert_success file-generate "${info[mount0]}/dir/file_$size"
done

# Turn off two of three server rooms
for csid in {0..5}; do
	assert_success lizardfs_chunkserver_daemon "$csid" stop &
done
wait

# Verify if all the files survived
assert_success file-validate  "${info[mount0]}/dir/file_"*
