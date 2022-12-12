# The following line is needed in the sudoers to run this test
# lizardfstest ALL = NOPASSWD: /usr/bin/timeout 3 cat .oplog

CHUNKSERVERS=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

echo "Test file content" > file.txt

oplog_output_file="${TEMP_DIR}/oplog.txt"

sudo timeout 3 cat .oplog > ${oplog_output_file} &

# make sure that above command is already executed in the background
sleep 1

# generate some oplog output
ls -l
cat file.txt

wait	# wait for the timeout command

gid=$(id -g)
id

for fscall in "open" "read" "flush" "lookup" "getattr" "getxattr"
do
	matches=$(cat ${oplog_output_file} | grep "cmd:${fscall}" | grep -v "OPLOG" | grep -o "gid:[0-9]*" | cut -d ':' -f2)

	for match in ${matches}
	do
		echo "${fscall}: ${match}"
		expect_equals "${gid}" "${match}"
	done
done
