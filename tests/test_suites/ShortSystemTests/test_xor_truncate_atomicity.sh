timeout_set '1 minute'

CHUNKSERVERS=10 \
	DISK_PER_CHUNKSERVER=1 \
	MOUNTS=5 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

# Create a source file -- a valid file-generated file which consists of 200 kB of data
source="$RAMDISK_DIR/source"
FILE_SIZE=200K file-generate "$source"

cd "${info[mount0]}"
for level in 2 3 4 7 9; do
	# Create a file which consists of 400 kB of random data
	file="file$level"
	touch "$file"
	mfssetgoal xor$level "$file"
	head -c 400K /dev/urandom > "$file"

	# Run in parallel 200 dd processes, each copies different 1 kB of data from the source file
	# Distribute these processes between 3 first mountpoints (mount0, mount1, mount2)
	for i in {0..199}; do
		( sleep 0.$((2 * i + 100)) && dd if="$source" of="${info[mount$((i%3))]}/$file" \
				bs=1KiB skip=$i seek=$i count=1 conv=notrunc 2>/dev/null ) &
	done
	# In the meanwhile, shorten file from 400K to 200K, chopping 1 kB in each of 200 steps
	# Sometimes share mountpoint with dd (mount2), sometimes not (mount3, mount4)
	for i in {399..200}; do
		truncate -s ${i}K "${info[mount$((2 + i % 3))]}/$file"
	done
	wait # Wait for all dd processes to finish

	# Now file should be equal to the source file. Let's validate it!
	MESSAGE="Testing xor level $level" assert_success file-validate "$file"
done

# Remove part 1 of each chunk to verify parity parts
find_all_chunks -name "*xor_1_of*" | xargs rm -vf
MESSAGE="Verification of parity parts" expect_success file-validate file*
