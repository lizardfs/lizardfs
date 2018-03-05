awkscript='
/:$/ { next }   # skip filenames

/^\tchunk/ {
	chunkid = $3
	dir = substr(chunkid, 11, 2)
	next
}
!/^\t\tcopy/ {
	printf("UNKNOWN LINE: %s\n", $0)
	exit
}
/part 1\/[1-9] of xor/ {
	split($3, server, ":")
	sub(/xor/, "", $7)
	printf "CS%s/chunks%s/chunk_xor_parity_of_%s_%s.liz\n", server[2], dir, $7, chunkid
	next
}
/part [2-9]\/[2-9] of xor/ {
	split($3, server, ":")
	sub(/xor/, "", $7)
	printf "CS%s/chunks%s/chunk_xor_%d_of_%d_%s.liz\n", server[2], dir, $5-1, $7, chunkid
	next
}
/part [1-9]\/[2-9] of ec\(3\,2\)/ {
	split($3, server, ":")
	printf "CS%s/chunks%s/chunk_ec2_%d_of_3_2_%s.liz\n", server[2], dir, $5, chunkid
	next
}
{
	split($3, server, ":")
	printf "CS%s/chunks%s/chunk_%s.liz\n", server[2], dir, chunkid
	next
}
'

CHUNKSERVERS=5 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	CHUNKSERVER_EXTRA_CONFIG="CREATE_NEW_CHUNKS_IN_MOOSEFS_FORMAT=0" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

files=()
for goal in 1 2 3 xor2 xor3 ec32; do
	file="file_goal_$goal"
	touch "$file"
	lizardfs setgoal "$goal" "$file"
	dd if=/dev/zero of="$file" bs=1MiB count=5 seek=62 conv=notrunc
	truncate -s 100M "$file" # Increases version of the second chunk
	files+=("$file")
done

chunks_info=$(lizardfs fileinfo "${files[@]}" \
		| awk "$awkscript" \
		| sed -e "s|CS${info[chunkserver0_port]}|$(cat ${info[chunkserver0_hdd]})|" \
		| sed -e "s|CS${info[chunkserver1_port]}|$(cat ${info[chunkserver1_hdd]})|" \
		| sed -e "s|CS${info[chunkserver2_port]}|$(cat ${info[chunkserver2_hdd]})|" \
		| sed -e "s|CS${info[chunkserver3_port]}|$(cat ${info[chunkserver3_hdd]})|" \
		| sed -e "s|CS${info[chunkserver4_port]}|$(cat ${info[chunkserver4_hdd]})|" \
		| sort)

chunks_real=$(find_all_chunks | sort)

expect_equals "$chunks_real" "$chunks_info"
