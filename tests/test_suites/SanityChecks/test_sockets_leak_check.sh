CHUNKSERVERS=1 \
	DISK_PER_CHUNKSERVER=1 \
	USE_RAMDISK="yes" \
	setup_local_empty_lizardfs info

max_files=10
max_sockets_delta=100
time_limit=10

get_used_socket_count() {
	ss -s | grep "TCP:" | awk '{print $2}'
}

if ! is_program_installed ss; then
	test_fail "ss not installed"
fi

used_socket_count_at_start=$(get_used_socket_count)

for ((files_created=0; files_created < max_files; ++files_created)); do
	tmp_file=$(mktemp -p ${info[mount0]})
	dd if=/dev/zero of=$tmp_file bs=33 count=50000 2> /dev/null &
done

sleep $time_limit

sockets_delta=$(($(get_used_socket_count)-used_socket_count_at_start))

if ((sockets_delta >= max_sockets_delta)); then
	test_add_failure "$sockets_delta sockets are not closed"
fi
