assert_program_installed pv
assert_program_installed socat

# start_proxy <listen> <forward>
# Starts a proxy which listens on <listen> port, forwards all the traffic to <forward> port
# and limits rate of replies to 75 KiB/s
start_proxy() {
	local ip=$(get_ip_addr)
	local limiter_script="socat stdio tcp\:$ip\:$2\,rcvbuf=1024\,sndbuf=1024 | pv -qL 75k"
	( socat "tcp-listen:$1,reuseaddr,rcvbuf=1024,sndbuf=1024" "system:${limiter_script}" & )
}

CHUNKSERVERS=1 \
	MASTERSERVERS=2 \
	USE_RAMDISK="YES" \
	setup_local_empty_lizardfs info

m=${info[master0_data_path]} # master's working directory
s=${info[master1_data_path]} # shadow's working directory

# Create a proxy which will limit transfter rate of changelogs from master to shadow
# and modify shadow's config to use this proxy.
get_next_port_number proxy_port
start_proxy "$proxy_port" "${info[matoml]}"
sed -i -e "s/^MASTER_PORT.*/MASTER_PORT = $proxy_port/" "${info[master1_cfg]}"

# Start shadow master and wait until it synchronizes
assert_success lizardfs_master_n 1 start
assert_eventually 'test -e "$s/metadata.mfs.1"'

# Generate about 1 MiB of entries and terminate the master server.
# Expect that it will wait until all the changes are sent to the shadow master.
mkdir "${info[mount0]}"/directory_with_name_long_enough_to_generate_a_big_changelog_entry{1..10000}
assert_success lizardfs_master_daemon stop

# Socket buffers need to be flushed, files have to be flushed, etc. Needs a couple of seconds more.
assert_eventually 'cmp <(get_changes "$m" |tail) <(get_changes "$s" |tail) &>/dev/null'
