CHUNKSERVERS=1 \
	DISK_PER_CHUNKSERVER=1 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="lfscachemode=NEVER" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1|REPLICATIONS_DELAY_INIT = 0" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

# Create a file
FILE_SIZE=123456 file-generate file

# Restarft the chunkserver and overwrite the file
lfschunkserver -c "${info[chunkserver0_config]}" restart
sleep 1
FILE_SIZE=234567 file-generate "$TEMP_DIR/newfile"
dd if="$TEMP_DIR/newfile" of=file conv=notrunc

# Check if there are chunks with version different than 1
hdd=$(cat "${info[chunkserver0_hdd]}")
number_of_chunks=$(find "$hdd" -name 'chunk*.lfs' | grep -v '_00000001[.]lfs' | wc -l)
if (( number_of_chunks != 1 )); then
	test_fail "Chunk didn't change version after modifying"
fi

# Check if we can read the modified chunk
file-validate file
