assert_program_installed setfacl

USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
touch file

export MESSAGE="Testing minimal ACLs for mask 750"
chmod 750 file
acls=$(getfacl -cE file)
mask=$(stat --format=%A file)
assert_equals '-rwxr-x---' "$mask"
assert_awk_finds '/^user::rwx$/' "$acls"
assert_awk_finds '/^group::r-x$/' "$acls"
assert_awk_finds '/^other::---$/' "$acls"
assert_awk_finds_no '/^mask:/' "$acls"

export MESSAGE="Testing results of setfacl -m other::r--"
setfacl -m other::r-- file
acls=$(getfacl -cE file)
mask=$(stat --format=%A file)
assert_equals "-rwxr-xr--" "$mask"
assert_awk_finds '/^other::r--$/' "$acls"
assert_awk_finds_no '/^mask:/' "$acls"

export MESSAGE="Testing results of setfacl -m group:fuse:rwx"
setfacl -m group:fuse:rwx file
acls=$(getfacl -cE file)
mask=$(stat --format=%A file)
assert_equals "-rwxrwxr--" "$mask"
assert_awk_finds '/^user::rwx$/' "$acls"
assert_awk_finds '/^group::r-x$/' "$acls"
assert_awk_finds '/^other::r--$/' "$acls"
assert_awk_finds '/^group:fuse:rwx$/' "$acls"
assert_awk_finds '/^mask::rwx$/' "$acls"

export MESSAGE="Testing results of chmod 700"
chmod 700 file
acls=$(getfacl -cE file)
mask=$(stat --format=%A file)
assert_equals "-rwx------" "$mask"
assert_awk_finds '/^user::rwx$/' "$acls"
assert_awk_finds '/^group::r-x$/' "$acls"
assert_awk_finds '/^other::---$/' "$acls"
assert_awk_finds '/^group:fuse:rwx$/' "$acls"
assert_awk_finds '/^mask::---$/' "$acls"

export MESSAGE="Testing permissions of a copied file"
cp -a file copy
expect_equals "$(getfacl -cE file | sort)" "$(getfacl -cE copy | sort)"
expect_equals "$(stat --format=%A file)" "$(stat --format=%A copy)"

export MESSAGE="Testing results of setfacl -b (removing all ACLs)"
setfacl -b file
acls=$(getfacl -cE file)
mask=$(stat --format=%A file)
assert_equals "-rwx------" "$mask"
assert_awk_finds '/^user::rwx$/' "$acls"
assert_awk_finds '/^group::---$/' "$acls"
assert_awk_finds '/^other::---$/' "$acls"
assert_awk_finds_no '/^group:fuse/' "$acls"
assert_awk_finds_no '/^mask:/' "$acls"
