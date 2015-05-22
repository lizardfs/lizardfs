timeout_set 60 minutes
continuous_test_begin
mfssetgoal 2 .

# Validate and generate a buch of files.
# This test will maintain up tp 1000 files, each between 1 KiB and 600 MiB.
for i in {1..30}; do
	subdir="subdir_$(random 1 20)"
	mkdir -p "$subdir"

	file="$subdir/file_$(random 1 50)"
	size=$(random 1K "$((i * 20))M")
	if [[ -e "$file" ]]; then
		# File exists. Verify it, resize and rewrite it.
		assert_success file-validate "$file"
		assert_success mv -v "$file" "$file.tmp"
		assert_success truncate -s "$size" "$file.tmp"
		assert_success file-overwrite "$file.tmp"
		assert_success mv -v "$file.tmp" "$file"
	else
		# File doesn't exist. Create it.
		FILE_SIZE=$size assert_success file-generate "$file.tmp"
		assert_success mv -v "$file.tmp" "$file"
	fi
	# Randomly change goal of the file.
	assert_success mfssetgoal "$(random 2 4)" "$file"
done
