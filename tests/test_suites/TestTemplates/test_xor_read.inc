# Code needs variables:
# XOR_LEVEL (2-9)
# READ_MODE (once_without_parity / once_from_parity / all_combinations / corrupted_part_1
#         / corrupted_parity_part)
# FILE_SIZE

CHUNKSERVERS=$((XOR_LEVEL + 1)) \
	DISK_PER_CHUNKSERVER=1 \
	USE_RAMDISK=yes \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 0" \
	setup_local_empty_lizardfs info

cd ${info[mount0]}

chunkservers_hdds=($(cat ${info[chunkserver0_hdd]}))
for i in $(seq 1 ${XOR_LEVEL}); do
	mfschunkserver -c "${info[chunkserver${i}_config]}" stop &
	chunkservers_hdds+=($(cat ${info[chunkserver${i}_hdd]}))
done
wait

FILE_SIZE=$FILE_SIZE BLOCK_SIZE=65536 file-generate file
mfschunkserver -c "${info[chunkserver0_config]}" stop

convert_to_xor_and_place_all_chunks $XOR_LEVEL "${chunkservers_hdds[0]}" "${chunkservers_hdds[@]}"
remove_standard_chunks "${chunkservers_hdds[@]}"

for i in $(seq 0 ${XOR_LEVEL}); do
	mfschunkserver -c "${info[chunkserver${i}_config]}" start &
done
wait

case "$READ_MODE" in
	once_without_parity)
		killed_chunkservers=0
		for i in $(seq 0 ${XOR_LEVEL}); do
			hdd=$(cat "${info[chunkserver${i}_hdd]}")
			if [ $(find "$hdd" -name 'chunk_xor_parity_of_*.mfs') ]; then
				mfschunkserver -c "${info[chunkserver${i}_config]}" stop
				killed_chunkservers=$((++killed_chunkservers))
			fi
		done
		if ((killed_chunkservers != 1)); then
			test_add_failure "Unexpected chunks placement in installation (${killed_chunkservers})"
		fi

		if ! file-validate file; then
			test_add_failure "Data read from file is different than written"
		fi
	;;
	once_from_parity)
		killed_chunkservers=0
		for i in 0 1; do # should be enough to check any 2 chunkservers
			hdd=$(cat "${info[chunkserver${i}_hdd]}")
			# check if the CS doesn't store a parity file (and therefore stores ordinary chunk part)
			if [ -z $(find "$hdd" -name 'chunk_xor_parity_of_*.mfs') ]; then
				mfschunkserver -c "${info[chunkserver${i}_config]}" stop
				killed_chunkservers=$((++killed_chunkservers))
				break
			fi
		done
		if ((killed_chunkservers != 1)); then
			test_add_failure "Unexpected chunks placement in installation (${killed_chunkservers})"
		fi

		if ! file-validate file; then
			test_add_failure "Data read from file is different than written"
		fi
	;;
	all_combinations)
		for i in $(seq 0 ${XOR_LEVEL}); do
			mfschunkserver -c "${info[chunkserver${i}_config]}" stop
			if ! file-validate file; then
				test_add_failure "Data read from file is different than written"
			fi
			mfschunkserver -c "${info[chunkserver${i}_config]}" start
		done
	;;
	corrupted_*)
		if [[ $READ_MODE == corrupted_part_1 ]]; then
			hdd=$(cat "${info[chunkserver1_hdd]}")
			chunk=$(find "$hdd" -name 'chunk_xor_1_of_?_*.mfs')
		elif [[ $READ_MODE == corrupted_parity_part ]]; then
			hdd=$(cat "${info[chunkserver0_hdd]}")
			chunk=$(find "$hdd" -name 'chunk_xor_parity_of_?_*.mfs')
		else
			test_add_failure "Wrong READ_MODE value = $READ_MODE"
		fi

		chunk_count=$(echo "$chunk" | wc -l)
		if [[ $chunk_count != 1 ]]; then
			test_fail "Test configuration error"
		fi

		echo aaaa | dd of="$chunk" bs=1 count=4 seek=6k conv=notrunc

		if ! file-validate file; then
			test_add_failure "Data read from file is different than written"
		fi
	;;
	*)
		test_add_failure "Wrong READ_MODE value = $READ_MODE"
	;;
esac
