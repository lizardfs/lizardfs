USE_RAMDISK=YES \
MASTERSERVERS=2 \
	setup_local_empty_lizardfs info

list_metadata_servers() {
	lizardfs_probe_master list-metadataservers --porcelain
}

nr="[0-9]+"
ip="($nr.){3}$nr"
version="$LIZARDFS_VERSION"
meta="$nr"
host="$(hostname)"
master_expected_state="^$ip ${info[matocl]} $host master running $meta $version\$"
shadow_expected_state="^$ip ${info[master1_matocl]} $host shadow connected $meta $version\$"

assert_matches "$master_expected_state" "$(list_metadata_servers)"

lizardfs_master_n 1 start
assert_eventually_prints 2 'list_metadata_servers | wc -l'
assert_matches "$master_expected_state" "$(list_metadata_servers | grep -w master)"
assert_eventually_matches "$shadow_expected_state" 'list_metadata_servers | grep -w shadow'

lizardfs_master_n 1 stop
assert_eventually_matches "$master_expected_state" 'list_metadata_servers'
