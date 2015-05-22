timeout_set 240 minutes  # Must be high when the 30G file is validated

# verify_archive <tar-file>
# Unpacks a tar archive and file-validates every file from this archive
verify_archive() {
	local archive=$1
	MESSAGE="Verifying archive $archive"

	# Unpack
	assert_success mkdir work/verify
	assert_success tar -xf "$archive" -C work/verify

	# Verify if there are any files and verify them
	local files=$(find work/verify -type f)
	assert_less_than 4 $(echo "$files" | wc -l)
	local file
	for file in $files; do
		assert_success file-validate "$file"
	done

	# Clean up
	assert_success rm -rf work/verify
	unset MESSAGE
}

# Prepare environment for the test - a directory for tar backups and a directory for temporary files
continuous_test_begin
mkdir -p backups work
mfssetgoal 2 work
mfssetgoal 3 backups
mfssettrashtime 1800 work
assert_success rm -rf work/*

# Create about 200 MB of files in the work/ subdir
path=$(date +%Y/%m/%d/%H)
assert_success mkdir -p "work/$path"
for sizelimit in 1K 10K 100K 900K 1M 2M 3M 5M 18M 20M 50M 100M 200M; do
	size=$(random 1K "$sizelimit")
	filename="work/$path/file_$(date +%s)_${size}B.bin"
	FILE_SIZE=$size assert_success file-generate "$filename"
done

# Append all generated files from the work/ directory to a couple of tar archives
for size in 1G 5G 30G; do
	archive="backups/backup_$size.tar"
	assert_success touch "$archive"  # Create if not exists
	assert_success mfsmakesnapshot "$archive" work/tmp.tar
	assert_success tar -f work/tmp.tar --append --seek -v -C work "$path"
	assert_success mv -v work/tmp.tar "$archive"

	# If the archive is big enough (i.e., >= $size)...
	if [[ $(stat --format %s "$archive") -gt $(parse_si_suffix "$size") ]]; then
		# Verify this archive and its oldest sister
		verify_archive "$archive"
		if [[ -e "$archive.9" ]]; then
			verify_archive "$archive.9"
		fi
		# Rotate archives: archive -> archive.1 -> archive.2 -> ... -> archive.9
		for i in {8..1}; do
			if [[ -e "$archive.$((i))" ]]; then
				assert_success mv -v "$archive.$((i))" "$archive.$((i+1))"
			fi
		done
		assert_success mv -v "$archive" "$archive.1"
	fi
done

# Clean up
assert_success rm -rf work/*
