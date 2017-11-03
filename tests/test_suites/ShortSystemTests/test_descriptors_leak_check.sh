timeout_set 1 minute

CHUNKSERVERS=1 \
	DISK_PER_CHUNKSERVER=1 \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

max_files=100
max_open_descriptors=10
time_limit=45
for ((files_created=0; files_created < max_files; ++files_created)); do
	tmp_file=$(mktemp -p ${info[mount0]})
	dd if=/dev/zero of=$tmp_file bs=33 count=1000 2> /dev/null &
done

wait

# wait for lizardfs to close files
cs_pid=$(lizardfs_chunkserver_daemon 0 test 2>&1 | sed 's/.*: //')
for ((time_elapsed=0; time_elapsed < time_limit; ++time_elapsed)); do
	leaked_descriptors_number=$(lsof +D $RAMDISK_DIR -p$cs_pid 2>/dev/null | \
			grep -v 'lock' | grep chunk_ | wc -l)
	if ((leaked_descriptors_number < max_open_descriptors)); then
		break
	fi
	if ! ((time_elapsed % 10)); then
		echo Open descriptors: $((leaked_descriptors_number))
	fi
	sleep 1
done
echo Open descriptors: $((leaked_descriptors_number))

if ((leaked_descriptors_number >= max_open_descriptors)); then
	test_add_failure "$leaked_descriptors_number files are not closed"
fi

