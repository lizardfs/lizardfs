assert_program_installed attr

CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	setup_local_empty_lizardfs info

cd "${info[mount0]}"
mkdir dir
echo a > file
ln -s file file_link

name1=$'name1\n)(*&\t^%$ #@!`'
name2="name2"
name3="$(base64 -w 0 /dev/urandom | head -c 250)"  # attr can't set >250B name (but doc says 256B)
name4="$(base64 -w 0 /dev/urandom | head -c 257)"
name5="name5"
name6="name6"
name7="name7"

value1="some small value"
value2='Lorem ipsum dolor sit amet ~!<>@#;:"$}{][\|%^&*()'
value3=""
value4=""
value5=""
value6="$(base64 -w 0 /dev/urandom | head -c 65536)"
value7="$(base64 -w 0 /dev/urandom | head -c 65537)"

expect_success attr -qs "$name1" -V "$value1" .
expect_success attr -qs "$name2" -V "$value2" file
expect_success attr -qs "$name3" -V "$value3" file
expect_failure attr -qs "$name4" -V "$value4" file
expect_success attr -qLs "$name5" -V "$value5" file_link
expect_success attr -qs "$name6" -V "$value6" dir
expect_failure attr -qs "$name7" -V "$value7" dir

lizardfs_master_daemon restart

expect_equals "$name1" "$(attr -ql .)"
expect_equals "$(echo -e "$name2\n$name3\n$name5" | sort)" "$(attr -ql file | sort)"
expect_equals "$(echo -e "$name2\n$name3\n$name5" | sort)" "$(attr -qLl file_link | sort)"
expect_equals "$name6" "$(attr -ql dir)"
expect_equals "$value1" "$(attr -qg "$name1" .)"
expect_equals "$value2" "$(attr -qg "$name2" file)"
expect_equals "$value2" "$(attr -qLg "$name2" file_link)"
expect_equals "$value3" "$(attr -qg "$name3" file)"
expect_equals "$value5" "$(attr -qLg "$name5" file_link)"
expect_equals "$value6" "$(attr -qg "$name6" dir)"
