timeout_set 4 minutes
assert_program_installed wget tidy

CHUNKSERVERS=3 \
	DISK_PER_CHUNKSERVER=3 \
	CGI_SERVER="YES" \
	MOUNTS=3 \
	USE_RAMDISK="YES" \
	CHUNKSERVER_LABELS="0,1:de|2:us" \
	MASTER_CUSTOM_GOALS="11 11: de de|12 12: us us|13 13: us de|18 18: us _ _|19 19: _ us de" \
	MOUNT_0_EXTRA_CONFIG="mfscachemode=NEVER,mfsreportreservedperiod=1,mfsdirentrycacheto=0" \
	MOUNT_1_EXTRA_CONFIG="mfsmeta" \
	MFSEXPORTS_EXTRA_OPTIONS="allcanchangequota,ignoregid" \
	MFSEXPORTS_META_EXTRA_OPTIONS="nonrootmeta" \
	MOUNT_2_EXTRA_EXPORTS="mingoal=1,maxgoal=10,maxtrashtime=2w" \
	setup_local_empty_lizardfs info

# Save path of meta-mount in MFS_META_MOUNT_PATH for metadata generators
export MFS_META_MOUNT_PATH=${info[mount1]}

# Save path of changelog.mfs in CHANGELOG to make it possible to verify generated changes
export CHANGELOG="${info[master_data_path]}"/changelog.mfs

# A function which downloads all pages from the CGI and stores them in a given directory.
# Usage: traverse_cgi <directory>
traverse_cgi() {
	local dir=$1
	local filter=""
	mkdir -p "$dir"
	if wget --version | awk 'NR==1 && $3 < 1.14 {exit 1}' ; then
		# Optimization for newer wgets -- don't download pages with two tabs open.
		# This is achieved by filtering out URLs with '|' charecters (like in secions=IN|CS|MO).
		filter="--reject-regex=%7C"
	fi
	wget -l2 --directory-prefix="$dir" $filter -r -q "${info[cgi_url]}"
}

# A directory where pages from CGI will be saved
cgi_pages="$RAMDISK_DIR/cgi_pages"

# Download data from cgi before creating any files
traverse_cgi "$cgi_pages/empty"

# Generate some metadata and stop one chunkserver to make the CGI interface interesting
cd "${info[mount0]}"
metadata_generate_all
lizardfs_chunkserver_daemon 0 stop

# Download data from cgi after creating some files
traverse_cgi "$cgi_pages/full"

# Make sure mfscgiserv connected to the master server and downloaded more than 20 files.
# Only a few files are downloaded when connection to the master server was unsuccessful.
assert_less_than '20' "$(find "$cgi_pages/empty" -name "mfs.cgi*" | wc -l)"
assert_less_than '20' "$(find "$cgi_pages/full" -name "mfs.cgi*" | wc -l)"

# Make sure there are no tracebacks.
expect_empty "$(grep -Inri -A 20 'Traceback' "$cgi_pages" || true)"

# Validate html pages using 'tidy'
find "$cgi_pages" -name "mfs.cgi*" | while read file; do
	MESSAGE="Validating $file" assert_empty "$(tidy -q -errors $file 2>&1)"
done
