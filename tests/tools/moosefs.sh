build_moosefs_or_use_cache() {
	local patch_path="${SOURCE_DIR}"/tests/tools/moosefs_valgrind.patch

	# Exit if MooseFS was already configured and installed,
	# assume it was configured properly
	(cd "$MOOSEFS_DIR/src/mfs-1.6.27" && make install) && return || true

	rm -rf "$MOOSEFS_DIR"
	mkdir -p "$MOOSEFS_DIR"
	pushd "$MOOSEFS_DIR"
	mkdir src
	cd src
	wget http://moosefs.org/tl_files/mfscode/mfs-1.6.27-5.tar.gz
	tar xzf mfs-1.6.27-5.tar.gz
	cd mfs-1.6.27
	patch -p1 < $patch_path
	./configure --prefix="$MOOSEFS_DIR"
	make install
	popd
}

test_moosefs() {
	test -x "$MOOSEFS_DIR/bin/mfsmount"
	test -x "$MOOSEFS_DIR/sbin/mfschunkserver"
	test -x "$MOOSEFS_DIR/sbin/mfsmaster"
}

build_moosefs() {
	build_moosefs_or_use_cache
	test_moosefs
}

moosefs_chunkserver_daemon() {
	"$MOOSEFS_DIR/sbin/mfschunkserver" -c "${lizardfs_info_[chunkserver$1_cfg]}" "$2" | cat
	return ${PIPESTATUS[0]}
}

moosefs_master_daemon() {
	"$MOOSEFS_DIR/sbin/mfsmaster" -c "${lizardfs_info_[master_cfg]}" "$1" | cat
	return ${PIPESTATUS[0]}
}

# A generic function to run MooseFS commands. Usage examples:
# mfs mfssetgoal 3 file
# mfs mfsdirinfo file
# mfs mfsmetalogger stop
mfs() {
	local command="$1"
	shift
	"$MOOSEFS_DIR/"*bin"/$command" "$@" | cat
	return ${PIPESTATUS[0]}
}
