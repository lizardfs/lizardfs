# Set up an installation with three disks per chunkserver
USE_RAMDISK=YES \
	CHUNKSERVERS=1 \
	MASTER_EXTRA_CONFIG="PERSONALITY=ha-cluster-managed"
	MASTER_START_PARAM="-o ha-cluster-managed -o initial-personality=master" \
	SHADOW_START_PARAM="-o ha-cluster-managed -o initial-personality=shadow"
	setup_local_empty_lizardfs info

commands=(stop start restart reload isalive kill)

# Check that master refuses to cooperate without ha-cluster-managed parameter
for command in ${commands[@]}; do
	assert_failure lizardfs_master_daemon ${command}
done

# Check that master cooperates when ha-cluster-managed parameter is present
lizardfs_master_daemon_ha () {
	local cmd=$1
	shift
	lizardfs_master_daemon ${cmd} -o ha-cluster-managed $@
}

assert_success lizardfs_master_daemon_ha stop
assert_success lizardfs_master_daemon_ha start -o initial-personality=master
assert_success lizardfs_master_daemon_ha restart -o initial-personality=master
for command in ${commands[@]:3}; do
	assert_success lizardfs_master_daemon_ha ${command}
done
