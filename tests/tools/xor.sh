# 1: xor level
# 2: chunk filename
# 3: xor chunks destination
# 4: destination of chunk parity part
# 5-...: destination of chunk xor parts
convert_to_xor_and_place_single_chunk() {
	if ! is_program_installed chunk_converter; then
		test_fail "chunk_converter not installed"
	fi
	chunk_converter $1 "$2" "$3"

	local chunk_id_version_extension=${2: -29:29}
	if [[ $4 != $3 ]]; then
		mv "${3}/chunk_xor_parity_of_${1}_${chunk_id_version_extension}" \
			"${4}/chunk_xor_parity_of_${1}_${chunk_id_version_extension}" -f
	fi

	for i in $(seq 1 $1); do
		local varnum=$((i+4))
		mv "${3}/chunk_xor_${i}_of_${1}_${chunk_id_version_extension}" \
				"${!varnum}/chunk_xor_${i}_of_${1}_${chunk_id_version_extension}" -f
	done
}


# 1: xor level
# 2: source cs folder
# 3-...: destination cs folders (parity part, next parts)
convert_to_xor_and_place_all_chunks() {
	local xor_level=$1
	local source_hdd=$2
	local destination_hdds_count=$(($#-2))
	if (( destination_hdds_count != xor_level+1 )); then
		test_fail "Incorrect usage 'convert_to_xor_and_place_all_chunks'. Bad number of arguments!"
	fi

	shift 2
	find "$source_hdd" -name chunk_????????????????_????????.mfs | while read chunk; do
		local chunk_path=`dirname $chunk`
		local parent_path=`dirname $chunk_path`
		local parent_name=${chunk: -15:2}

		# XOR lvl / chunk / xors destination / path for parity part / paths for xor parts
		convert_to_xor_and_place_single_chunk ${xor_level} ${chunk} "${parent_path}" \
				${*/%/"/${parent_name}"}
	done
}

remove_standard_chunks() {
	for folder in "$@"; do
		find "$folder" -name 'chunk_????????????????_????????.mfs' | xargs -d'\n' -r rm -f
	done
}

remove_xor_chunks() {
	for folder in "$@"; do
		find "$folder" -name 'chunk_xor_*.mfs' | xargs -d'\n' -r rm -f
	done
}
