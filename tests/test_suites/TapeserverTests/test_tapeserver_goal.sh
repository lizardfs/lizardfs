CHUNKSERVERS=2 \
	USE_RAMDISK=YES \
	MASTER_CUSTOM_GOALS="5 tape: _ _ _@"
	setup_local_empty_lizardfs info

# Launch tapeserver at localhost
"$TAPESERVER" localhost ${info[matots]} &
# Wait for tapeserver to start
assert_eventually_prints 1 'lizardfs_probe_master list-tapeservers | wc -l'

# Create file, change its goal to 'tape' and check if it was correctly
# replicated to tapeserver
cd "${info[mount0]}"
FILE_SIZE=150K file-generate file
assert_equals 0 $(lizardfs fileinfo file | grep "tape replica" | wc -l)
lizardfs setgoal tape file
assert_eventually_prints 1 'mfsfileinfo file | grep "tape replica 1: OK" | wc -l'

