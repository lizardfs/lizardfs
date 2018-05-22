# Start an installation with 2 servers labeled 'hdd', 2 labeled 'ssd' and
# many custom goals, some of which require copies on servers which are not available.
# Goals named g2_{1..8} require 2 copies and goals named g3_{1..8} require 3 copies.
goals="1 g2_1: ssd ssd|2 g2_2: ssd hdd|3 g2_3: ssd _|4 g2_4: _ _`
		`|5 g2_5: ssd xxx|6 g2_6: xxx xxx|7 g2_7: xxx yyy|8 g2_8: xxx _`
		`|9 g3_1: ssd ssd hdd|10 g3_2: ssd ssd ssd|11 g3_3: ssd xxx xxx`
		`|12 g3_4: ssd ssd xxx|13 g3_5: xxx xxx xxx|14 g3_6: xxx _ _`
		`|15 g3_7: xxx yyy zzz|16 g3_8: ssd hdd xxx"
USE_RAMDISK=YES \
	CHUNKSERVERS=4 \
	CHUNKSERVER_LABELS="0,1:ssd|2,3:hdd" \
	MASTER_CUSTOM_GOALS=$goals \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_TIME = 1`
			`|CHUNKS_LOOP_MAX_CPU = 90`
			`|ACCEPTABLE_DIFFERENCE = 1.0`
			`|CHUNKS_WRITE_REP_LIMIT = 1`
			`|OPERATIONS_DELAY_INIT = 0`
			`|OPERATIONS_DELAY_DISCONNECT = 0" \
	setup_local_empty_lizardfs info
cd "${info[mount0]}"

# Create one file in every goal and remember the list of copies of its chunk
declare -A infos  # an array where lists of chunks will be stored
for goal in g{2..3}_{1..8}; do
	file="file_$goal"
	touch "$file"
	lizardfs setgoal "$goal" "$file"
	FILE_SIZE=1K file-generate "$file"

	# Now verify if the file has exactly the requied number of copies of its chunk
	fileinfo=$(lizardfs fileinfo "$file")
	expected_copies=${goal:1:1}
	actual_copies=$(echo "$fileinfo" | grep copy | wc -l)
	MESSAGE="New $file: $fileinfo" assert_equals "$expected_copies" "$actual_copies"

	# And remember this list of chunks in the 'infos' array for future use.
	infos[$file]=$fileinfo
done

# Wait a couple of chunk loops. Except no changes in the lists of copies.
sleep 5
for file in file*; do
	MESSAGE="Veryfing $file" expect_equals "${infos[$file]}" "$(lizardfs fileinfo "$file")"
done
