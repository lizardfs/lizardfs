timeout_set 6 minutes

CHUNKSERVERS=3 \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

if ! dbench --help &>/dev/null; then
	test_fail "dbench not installed"
fi

dbench_output=$TEMP_DIR/$(unique_file)
dbench -s -S -t 180 12 | tee "$dbench_output"
if [[ ${PIPESTATUS[0]} != 0 ]]; then
    test_fail "dbench failed"
fi

throughput=$(awk '/^Throughput [0-9.]+ MB.s/ {print $2}' $dbench_output)
if [[ $throughput ]]; then
	echo -e "Throughput\n$throughput" \
			> $TEST_OUTPUT_DIR/dbench_throughput_results.csv
else
	test_fail "Cannot parse dbench output"
fi
