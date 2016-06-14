CHUNKSERVERS=1 \
	MOUNTS=2 \
	USE_RAMDISK="YES" \
	MOUNT_0_EXTRA_CONFIG="mfscachemode=NEVER" \
	MOUNT_1_EXTRA_CONFIG="mfsmeta" \
	MFSEXPORTS_META_EXTRA_OPTIONS="nonrootmeta" \
	setup_local_empty_lizardfs info

# This determines number of files to be created in this test
file_suffixes=$(seq 300 | xargs echo)

# Create files for the test and move all of them to trash
cd "${info[mount0]}"
mkdir dir
for i in $file_suffixes; do
	echo content_$i > dir/file_$i
done
lizardfs settrashtime -r 3600 dir/
rm -rf dir/

# Create a directory where all the files from trash will be restored and recover them there
mkdir untrashed
trash="${info[mount1]}"/trash
for i in $file_suffixes; do
	MESSAGE="Recovering file #$i"
	echo untrashed/recovered_$i > "$trash"/*file_$i
	assert_eventually "test -e '$trash'/*recovered_$i" # rename in trash is asynchronous!
	assert_success mv "$trash"/*recovered_$i "$trash"/undel/
	# Verify if the operation was successful
	assert_file_exists untrashed/recovered_$i
	assert_equals content_$i "$(cat untrashed/recovered_$i)"
done
