CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="lfscachemode=NEVER" \
	setup_local_empty_lizardfs info

block_size=65536
cd ${info[mount0]}
for filesize in 30 90 $((9 * block_size)) $((15 * block_size - 30)) $((15 * block_size + 30)); do
	for goal in 1 3; do
		for diff in 30 $block_size $((block_size - 30)) $((block_size + 30)) \
				$((7 * block_size)) $((7 * block_size + 30)) $((7 * block_size - 30)); do
			export MESSAGE="Testing filesize=$filesize +/- $diff, goal $goal"
			echo "$MESSAGE"
			# Create a file and its two snapshots. File will not be modified in this test
			touch file
			lfssetgoal $goal file >/dev/null
			FILE_SIZE=$filesize file-generate file
			lfsmakesnapshot file snapshot1
			lfsmakesnapshot file snapshot2
			assert_success file-validate file snapshot1 snapshot2

			# Truncate one of these snapshots up and check if there were zeros added
			assert_success truncate -s $((filesize + diff)) snapshot1
			assert_success file-validate file # This file shouldn't be changed!
			expect_files_equal <(head -c $diff /dev/zero) <(tail -c $diff snapshot1)
			expect_files_equal <(head -c $filesize file) <(head -c $filesize snapshot1)
			# Revert the operation and check if no data is changed after this step
			assert_success truncate -s $filesize snapshot1
			assert_success file-validate file snapshot1 snapshot2

			# Truncate the second snapshot down (if anything would remain)
			if (( diff <= filesize )); then
				truncated=$((filesize - diff))
				assert_success truncate -s $truncated snapshot2
				assert_success file-validate file # This file shouldn't be changed!
				expect_files_equal <(head -c $truncated file) <(head -c $truncated snapshot2)
				# Now revert the original size, but it should be filled wih zeros
				assert_success truncate -s $filesize snapshot2
				assert_success file-validate file # This file shouldn't be changed!
				expect_files_equal <(head -c $truncated file) <(head -c $truncated snapshot2)
				expect_files_equal <(head -c $diff /dev/zero) <(tail -c $diff snapshot2)
			fi

			rm file snapshot1 snapshot2
		done
	done
done
