build_lizardfsXX_or_use_cache() {
	# Exit if MooseFS was already configured and installed,
	# assume it was configured properly
	(cd "$LIZARDFSXX_DIR/src/lizardfs/build" && make install) && return || true

	rm -rf "$LIZARDFSXX_DIR"
	mkdir -p "$LIZARDFSXX_DIR"
	pushd "$LIZARDFSXX_DIR"
	mkdir src
	cd src
	git clone https://github.com/lizardfs/lizardfs.git
	cd lizardfs
	git checkout v$LIZARDFSXX_TAG
	mkdir build
	cd build
	cmake .. -DCMAKE_INSTALL_PREFIX="$LIZARDFSXX_DIR"
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
	"$LIZARDFSXX_DIR/sbin/mfschunkserver" -c "${lizardfs_info_[chunkserver$1_config]}" "$2" | cat
	return ${PIPESTATUS[0]}
}

lizardfsXX_master_daemon() {
	"$LIZARDFSXX_DIR/sbin/mfsmaster" -c "${lizardfs_info_[master_cfg]}" "$1" | cat
	return ${PIPESTATUS[0]}
}

# A generic function to run MooseFS commands. Usage examples:
# mfs mfssetgoal 3 file
# mfs mfsdirinfo file
# mfs mfsmetalogger stop
lizardfsXX() {
	local command="$1"
	shift
	"$LIZARDFSXX_DIR/"*bin"/$command" "$@" | cat
	return ${PIPESTATUS[0]}
}
