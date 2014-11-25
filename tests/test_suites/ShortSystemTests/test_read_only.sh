timeout_set 2 minutes
assert_program_installed attr

MOUNTS=2 \
CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="lfsacl" \
	MOUNT_1_EXTRA_EXPORTS="ro,lfscachemode=NEVER" \
	LFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	setup_local_empty_lizardfs info

lizardfs_metalogger_daemon start

# Generate some metadata and remember it
cd "${info[mount0]}"
metadata_generate_all
FILE_SIZE=16M file-generate rw_file
expect_success setfacl -m mask::r rw_file
expect_success lfssetquota -u $(id -u) 5GB 10GB 0 0 .
metadata=$(metadata_print)

cd "${info[mount1]}"
assert_no_diff "$metadata" "$(metadata_print)"
metadata_validate_files

expect_success file-validate rw_file
expect_failure bash -c 'echo >> rw_file'
expect_failure touch ro_a
expect_failure mkdir ro_b
expect_failure truncate -s 256 rw_file
expect_failure dd if=/dev/zero of=rw_file bs=1k count=1
expect_failure mv rw_file ro_file
expect_failure rm rw_file
expect_failure ln -s rw_file ro_file
expect_failure ln rw_file ro_chunk_xyx
expect_failure lfssettrashtime 300 rw_file
expect_failure lfssetgoal 3 rw_file
expect_failure setfacl -m mask::rwx rw_file
expect_failure setfacl -b rw_file
expect_failure attr -s attr1 -V '' rw_file
expect_failure setfattr -n user.attr2 -v 'some value' rw_file
expect_failure mkfifo ro_fifo
expect_failure lfsmakesnapshot rw_file ro_file
expect_failure lfsfilerepair rw_file
expect_failure lfssetquota -u $(id -u) 10GB 30GB 0 0 .
expect_failure lfssetquota -u $(id -u) 0 0 0 0 .
expect_failure lfsappendchunks ro_file rw_file rw_file
expect_success file-validate rw_file

