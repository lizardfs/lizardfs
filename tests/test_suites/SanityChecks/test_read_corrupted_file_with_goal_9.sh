goal=9
CHUNKSERVERS=9 \
		MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
		CHUNKSERVER_EXTRA_CONFIG="HDD_TEST_FREQ = 10000" \
		USE_RAMDISK=YES \
		setup_local_empty_lizardfs info

cd "${info[mount0]}"
touch file
lizardfs setgoal $goal file
FILE_SIZE=1234567 file-generate file

hdds=()
for i in 0 $(seq 2 $((goal-1))); do
	hdds+=("$(cat "${info[chunkserver${i}_hdd]}")")
done

find "${hdds[@]}" -name 'chunk_*.???' | xargs -d'\n' -P10 -IXX \
		dd if=/dev/zero of=XX bs=1 count=4 seek=6k conv=notrunc

for i in $(seq 20); do
	if ! file-validate file; then
		test_fail "Fail at reading data from a file!"
	fi
done
