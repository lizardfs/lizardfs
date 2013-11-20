timeout_set 1 hour

CHUNKSERVERS=3 \
	MOUNTS=2 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"

if ! dbench --help &>/dev/null; then
	test_fail "dbench not installed"
fi

# Let dbench run for half an hour with 16 clients
start=$(date +%s)
if ! dbench -s -S -t 1800 16; then
	status=$?
	elapsed=$(($(date +%s)-start))
	test_add_failure "dbench returned status $? after $elapsed seconds"
fi
