timeout_set '6 minutes'

CHUNKSERVERS=1 \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

# This function prints some names of directories to be created
file_names() {
	local N=30000
	seq 1 $N
	seq 1 $N | sed -e "s/^/dir_/"
	seq 1 $N | sed -e "s/^/a bit longer name of a direcotry /"
	seq 1 $N | sed -e "s/^/$(yes loooooong | tr '\n' '_' | head -c 245)_/"
}

# Create a lot of directories and verify if ls returns them all
cd "${info[mount0]}"
file_names | xargs -d'\n' mkdir
assert_no_diff "$(file_names | sort)" "$(ls | sort)"
