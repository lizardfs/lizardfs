timeout_set 1 minute
assert_program_installed nc

MASTERSERVERS=2 \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

lizardfs_master_n 1 start
assert_eventually 'lizardfs_shadow_synchronized 1'

my_client() {
	local metaserver="$1"
	local ma_to_someone="$2"

	if [[ ${1} == 0 ]]; then
		local PORT=${info[${2}]}
	else
		local PORT=${info[master${1}_${2}]}
	fi
	echo "Connecting to metadata server ${1} ${2} port ${PORT}..."
	nc -d localhost ${PORT} | cat > ${TEMP_DIR}/nc_out
	local result="${PIPESTATUS[0]}"
	echo "$result" > ${TEMP_DIR}/${2}_exit_status
	echo "Connecting to metadata server ${1} ${2} result: $result"
}

run_my_client() {
	local assert="$1"
	local expected_status="$2"
	shift 2
	local metaserver="$1"
	local ma_to_someone="$2"

	my_client "$metaserver" "$ma_to_someone" &
	"$assert" wait_for "test -f ${TEMP_DIR}/${ma_to_someone}_exit_status" '5 seconds'
	assert_equals "$expected_status" `cat ${TEMP_DIR}/${ma_to_someone}_exit_status || echo 1`
}

# Shadow accepts matocl connections, but doesn't respond to most messages. TODO: verify it.
#run_my_client assert_success 0 1 matocl
run_my_client assert_success 0 1 matocs
run_my_client assert_success 0 1 matots
run_my_client assert_success 0 1 matoml

rm ${TEMP_DIR}/mato*_exit_status
lizardfs_master_daemon stop
lizardfs_make_conf_for_master 1
lizardfs_admin_shadow 1 reload-config

sleep 1
run_my_client assert_failure 1 0 matocl
run_my_client assert_failure 1 0 matocs
run_my_client assert_failure 1 0 matots
run_my_client assert_failure 1 0 matoml
