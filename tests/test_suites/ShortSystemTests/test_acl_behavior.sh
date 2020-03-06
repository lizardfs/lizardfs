timeout_set 2 minutes
assert_program_installed setfacl
touch "$TEMP_DIR/f"
MESSAGE="Testing ACL support in $TEMP_DIR/" assert_success setfacl -m group:fuse:rw "$TEMP_DIR/f"

USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

lizdir="${info[mount0]}/subdir"
tmpdir="$TEMP_DIR/subdir"
mkdir -p "$lizdir" "$tmpdir"
chmod 770 "$lizdir" "$tmpdir"

# Do the same things in two trees (lizdir and tmpdir) and compare results after each command
counter=0
while read command; do
	command=$(sed -e 's/ *#.*//' <<< "$command") # Strip the trailing comment
	export MESSAGE="Executing '$command' in both directory trees"
	( cd "$tmpdir" ; assertlocal_success eval "$command" )
	( cd "$lizdir" ; assertlocal_success eval "$command" )

	if [[ $((RANDOM % 10)) == 0 && $counter < 3 ]]; then
		lizardfs_master_daemon restart
		lizardfs_wait_for_all_ready_chunkservers
		counter=$((counter + 1))
	fi

	export MESSAGE="Veryfing permissions after '$command'"
	cd "$lizdir"
	find . | while read f; do
		assert_equals "$(stat --format=%A "$tmpdir/$f")" "$(stat --format=%A "$lizdir/$f")"
		assert_equals "$(getfacl -cpE "$tmpdir/$f" | sort)" "$(getfacl -cpE "$lizdir/$f" | sort)"
	done
done <<'END'
	mkdir minimal                      # Play with minimal ACL using setfacl and chmod
	setfacl -m group::rwx minimal
	chmod 400 minimal
	setfacl -m user::rwx minimal
	setfacl -m other::r-- minimal
	chmod 600 minimal
	chmod g+s minimal
	chmod u+s minimal
	chmod +t minimal
	rmdir minimal

	touch file_{a,b}                   # Create two files writeable to the group
	chmod 660 file_{a,b}
	setfacl -m mask::--- file_{a,b}    # Add extended ACL by setting a very restrictive mask
	setfacl -b file_a                  # Remove extended ACL from file_a by setfacl -b
	setfacl -x mask file_b             # Remove extended ACL from file_b by removing last entry
	rm -f file_{a,b}

	touch file                         # Create a file and....
	chmod 700 file                     # Test if chmod properly influences minimal ACL
	chmod 750 file
	chmod 650 file
	chmod 444 file
	chmod 750 file                     # After this change we start adding extended ACL
	setfacl -m user:lizardfstest:rwx file
	setfacl -m user:lizardfstest:rw- file
	setfacl -x user:lizardfstest file
	setfacl -m user:nobody:--- file
	setfacl -m group:adm:rwx file
	setfacl -m mask::rwx file
	setfacl -m mask::r-- file
	setfacl -m group::--- file
	setfacl -m group::rwx file
	setfacl -m other::r-- file
	setfacl -m user::rw- file
	chmod 700 file                     # Test if chmod properly influences extended ACL
	chmod 750 file
	chmod 650 file
	chmod 444 file
	chmod 750 file

	cp -a file copy                    # Test copying file with extended ACL
	setfacl -b copy                    # Test removing the all extended ACL entries
	chmod 700 copy                     # Test if chmod properly influences minimal ACL now
	chmod 666 copy
	chmod 750 copy

	cp -a file copy2                   # Create copy2 with ACL and remove all entries one by one
	setfacl -x user:nobody copy2
	setfacl -x group:adm copy2
	chmod 700 copy2                    # Only mask:: left now; test chmod on extended ACL
	chmod 666 copy2
	chmod 750 copy2
	setfacl -x mask copy2              # The last entry is removed
	chmod 444 copy2                    # Test if chmod properly influences minimal ACL now
	chmod 750 copy2

	mkdir dir                          # Test directory's ACL
	chmod a+x dir                      # Test if chmod properly influences dir's minimal ACL
	setfacl -m group:adm:rwx dir
	setfacl -d -m group:adm:rw- dir    # Set some default: entries
	setfacl -d -m group:games:--- dir
	chmod 770 dir                      # Test if chmod properly influences dir's extended ACL
	touch dir/file1                    # Create a file in directory with default: entries
	setfacl -b dir/file1
	( umask 117 ; touch dir/file2 )    # Test creating files with different create modes
	( umask 077 ; touch dir/file3 )
	ln file dir/link_file              # Test if links get proper permissions
	ln dir/file1 link_file1
	ln -s file symlink_file
	ln -s file1 dir/symlink_file1

	mkdir dir/subdir                   # Create a directory in directory with default: entries
	mkdir -m 700 dir/subdir2           # Test inheriting default permissions with some masks
	mkdir -m 755 dir/subdir3
	mkdir -m 772 dir/subdir2/subsubdir1
	( umask 000 ; mkdir dir/subdir4 )
	( umask 444 ; mkdir dir/subdir5 )
	touch dir/subdir/file4
	( umask 277 ; touch dir/subdir/file5 )
	touch dir/subdir/file6a
	setfacl -k dir/subdir              # Remove default: ACL entries
	touch dir/subdir/file6b

	cp file dir/file7                  # Test copying to directory with default: ACL
	cp -a file dir/file8
	cp dir/file8 dir/file9
	cp -a dir/file8 dir/file10
	cp -a file dir/subdir              # Test copying to directory without default: ACL
	cp -ar dir dir2                    # Test copying a directory
	cp -r dir dir2/dir3
END
