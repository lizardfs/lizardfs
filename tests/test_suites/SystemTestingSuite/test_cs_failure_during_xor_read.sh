start_proxy() {
	# Accept one connection from mfsmount on the fake port
	socat tcp-listen:$1,reuseaddr system:"
		socat stdio tcp\:$(get_ip_addr)\:$2 |  # connect to real server
		{
			dd bs=1k count=12k ;               # forward 12MB
			sleep 1d ;                         # and go catatonic
		}" &
}

if ! is_program_installed socat; then
	test_fail "Configuration error, please install 'socat'"
fi

timeout_set 1 minute

CHUNKSERVERS=3 \
	DISK_PER_CHUNKSERVER=1 \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

filename=${info[mount0]}/file
cs0_disk=$(cat ${info[chunkserver0_hdd]})
cs1_disk=$(cat ${info[chunkserver1_hdd]})
cs2_disk=$(cat ${info[chunkserver2_hdd]})

mfschunkserver -c "${info[chunkserver2_config]}" stop
mfschunkserver -c "${info[chunkserver1_config]}" stop
FILE_SIZE=123456789 BLOCK_SIZE=12345 file-generate $filename
mfschunkserver -c "${info[chunkserver0_config]}" stop

convert_to_xor_and_place_all_chunks 2 "$cs0_disk" "$cs0_disk" "$cs1_disk" "$cs2_disk"
remove_standard_chunks "$cs0_disk"

mfschunkserver -c "${info[chunkserver0_config]}" start
mfschunkserver -c "${info[chunkserver1_config]}" start

port=${info[chunkserver2_port]}
config=${info[chunkserver2_config]}
# Limit data transfer from chunkserver serving part 2
start_proxy $port $((port + 1000))
LD_PRELOAD="$LIZARDFS_ROOT/lib/libredirect_bind.so" mfschunkserver -c "${config}" start
sleep 3

if ! file-validate "$filename"; then
	test_add_failure "Data read from file is different than written"
fi
