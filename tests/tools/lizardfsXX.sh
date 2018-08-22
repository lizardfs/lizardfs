build_lizardfsXX_or_use_cache() {
	LIZARDFS_TESTS_DIR=$(pwd)
	# Exit if LizardFS was already configured and installed, assume it was
	# configured properly
	(cd "$LIZARDFSXX_DIR/src/lizardfs/build" && make install) && return || true

	rm -rf "$LIZARDFSXX_DIR"
	mkdir -p "$LIZARDFSXX_DIR"
	pushd "$LIZARDFSXX_DIR"
	mkdir src
	cd src
	git clone https://github.com/lizardfs/lizardfs.git
	cd lizardfs
	git checkout v$LIZARDFSXX_TAG
	for patch_name in "$LIZARDFS_TESTS_DIR"/patches/$LIZARDFSXX_TAG-*.patch; do
		if [ -f "$patch_name" ]; then
			patch -p1 < "$patch_name"
		fi
	done
	mkdir build
	cd build
	sed -i 's:add_subdirectory(src/mount/polonaise):# Polonaise disabled:g' ../CMakeLists.txt
	cmake .. -DCMAKE_INSTALL_PREFIX="$LIZARDFSXX_DIR" -DENABLE_POLONAISE=OFF
	make
	make install
	popd
}

test_lizardfsXX_executables() {
	test -x "$LIZARDFSXX_DIR/bin/mfsmount"
	test -x "$LIZARDFSXX_DIR/sbin/mfschunkserver"
	test -x "$LIZARDFSXX_DIR/sbin/mfsmaster"
}

build_lizardfsXX() {
	build_lizardfsXX_or_use_cache
	test_lizardfsXX_executables
}

lizardfsXX_chunkserver_daemon() {
	"$LIZARDFSXX_DIR/sbin/mfschunkserver" -c "${lizardfs_info_[chunkserver$1_cfg]}" "$2" | cat
	return ${PIPESTATUS[0]}
}

lizardfsXX_master_daemon() {
	"$LIZARDFSXX_DIR/sbin/mfsmaster" -c "${lizardfs_info_[master_cfg]}" "$1" | cat
	return ${PIPESTATUS[0]}
}

# A generic function to run LizardFS commands.
#
# Usage examples:
#   mfs mfssetgoal 3 file
#   mfs mfsdirinfo file
#   mfs mfsmetalogger stop
lizardfsXX() {
	local command="$1"
	shift
	"$LIZARDFSXX_DIR/"*bin"/$command" "$@" | cat
	return ${PIPESTATUS[0]}
}
