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
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

dir="${info[mount0]}/dir"
mkdir "$dir"
mfssetgoal xor2 "$dir"
FILE_SIZE=123456789 file-generate "$dir/file"

# Find any chunkserver serving part 1 of some chunk
csid=$(find_first_chunkserver_with_chunks_matching 'chunk_xor_1_of_2*')
port=${info[chunkserver${csid}_port]}
config=${info[chunkserver${csid}_config]}

# Limit data transfer from this chunkserver
start_proxy $port $((port + 1000))
mfschunkserver -c "${config}" stop
LD_PRELOAD="$LIZARDFS_ROOT/lib/libredirect_bind.so" mfschunkserver -c "${config}" start
sleep 1

if ! file-validate "$dir/file"; then
	test_add_failure "Data read from file is different than written"
fi
