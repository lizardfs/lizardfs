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
/parity/ {
	split($3, server, ":")
	printf "CS%s/chunks%s/chunk_xor_parity_of_%s_%s.mfs\n", server[2], dir, $5, chunkid
	next
}
/part/ {
	split($3, server, ":")
	split($5, part, "/")
	printf "CS%s/chunks%s/chunk_xor_%s_of_%s_%s.mfs\n", server[2], dir, part[1], part[2], chunkid
	next
}
{
	split($3, server, ":")
	printf "CS%s/chunks%s/chunk_%s.mfs\n", server[2], dir, chunkid
	next
}
'

CHUNKSERVERS=4 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

files=()
for goal in 1 2 3 xor2 xor3; do
	file="file_goal_$goal"
	touch "$file"
	mfssetgoal "$goal" "$file"
	dd if=/dev/zero of="$file" bs=1MiB count=5 seek=62 conv=notrunc
	truncate -s 100M "$file" # Increases version of the second chunk
	files+=("$file")
done

chunks_info=$(mfsfileinfo "${files[@]}" \
		| awk "$awkscript" \
		| sed -e "s|CS${info[chunkserver0_port]}|$(cat ${info[chunkserver0_hdd]})|" \
		| sed -e "s|CS${info[chunkserver1_port]}|$(cat ${info[chunkserver1_hdd]})|" \
		| sed -e "s|CS${info[chunkserver2_port]}|$(cat ${info[chunkserver2_hdd]})|" \
		| sed -e "s|CS${info[chunkserver3_port]}|$(cat ${info[chunkserver3_hdd]})|" \
		| sort)

chunks_real=$(find_all_chunks | sort)

expect_equals "$chunks_real" "$chunks_info"
