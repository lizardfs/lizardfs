timeout_set "1 minute"

# Start an installation with 2 servers labeled 'de', 2 labeled 'us' and one labeled 'cn'
USE_RAMDISK=YES \
	CHUNKSERVERS=5 \
	CHUNKSERVER_LABELS="0,1:de|2,3:us|4:cn" \
	MASTER_CUSTOM_GOALS="11 11: de de|12 12: us us|13 13: us de|14 14: de de de|15 15: us us us`
			`|16 16: us|17 17: us _|18 18: us _ _|19 19: _ us de|20 20: _ cn us de" \
	setup_local_empty_lizardfs info

# For each goal, define all possible lists of labels. Each list is sorted alphabetically,
# lists are separated using pipes, lables in a single list using commas.
expected_labels[1]="cn|de|us"
expected_labels[2]="cn,de|cn,us|de,de|de,us|us,us"
expected_labels[3]="cn,de,de|cn,de,us|cn,us,us|de,de,us|de,us,us"
expected_labels[4]="cn,de,de,us|cn,de,us,us|de,de,us,us"
expected_labels[5]="cn,de,de,us,us"
expected_labels[6]=${expected_labels[5]}
expected_labels[7]=${expected_labels[5]}
expected_labels[8]=${expected_labels[5]}
expected_labels[9]=${expected_labels[5]}
expected_labels[10]=${expected_labels[5]}
expected_labels[11]="de,de"
expected_labels[12]="us,us"
expected_labels[13]="de,us"
expected_labels[14]="cn,de,de|de,de,us" # There are only two 'de' servers!
expected_labels[15]="cn,us,us|de,us,us" # There are only two 'us' servers!
expected_labels[16]="us"
expected_labels[17]="cn,us|de,us|us,us"
expected_labels[18]="cn,de,us|cn,us,us|de,de,us|de,us,us"
expected_labels[19]="cn,de,us|de,de,us|de,us,us"
expected_labels[20]="cn,de,us,us|cn,de,de,us"

# For a given file, prints lables of chunkservers where the file's chunks are placed, eg. de,us,us
get_file_labels() {
	local fileinfo_to_labels="`
			`/copy .*:(${info[chunkserver0_port]}|${info[chunkserver1_port]}):.*\$/ {print \"de\"} `
			`/copy .*:(${info[chunkserver2_port]}|${info[chunkserver3_port]}):.*\$/ {print \"us\"} `
			`/copy .*:(${info[chunkserver4_port]}):.*\$/ {print \"cn\"}"
	lfsfileinfo "$1" | awk "$fileinfo_to_labels" | sort | tr '\n' ' ' | trim | tr ' ' ','
}

cd "${info[mount0]}"
for goal in "${!expected_labels[@]}"; do
	MESSAGE="Testing goal $goal: $(grep "^$goal " "${info[master_custom_goals]}" | cut -d' ' -f3-)"

	# Create a lot of files in the current goal and verify labels for each file
	mkdir "dir_$goal"
	lfssetgoal "$goal" "dir_$goal"
	for file in "dir_$goal/file"{1..50}; do
		echo x > "$file"
		assert_matches "^(${expected_labels[goal]})\$" "$(get_file_labels "$file")"
	done
done
