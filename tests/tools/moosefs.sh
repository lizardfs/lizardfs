build_lizardfs_or_use_cache() {
	local patch_path="${SOURCE_DIR}"/tests/tools/lizardfs_valgrind.patch

	# Exit if LizardFS was already configured and installed,
	# assume it was configured properly
	(cd "$LIZARDFS_DIR/src/lfs-1.6.27" && make install) && return || true

	rm -rf "$LIZARDFS_DIR"
	mkdir -p "$LIZARDFS_DIR"
	pushd "$LIZARDFS_DIR"
	mkdir src
	cd src
	wget http://lizardfs.org/tl_files/lfscode/lfs-1.6.27-5.tar.gz
	tar xzf lfs-1.6.27-5.tar.gz
	cd lfs-1.6.27
	patch -p1 < $patch_path
	./configure --prefix="$LIZARDFS_DIR"
	make install
	popd
}

test_lizardfs() {
	test -x "$LIZARDFS_DIR/bin/lfsmount"
	test -x "$LIZARDFS_DIR/sbin/lfschunkserver"
	test -x "$LIZARDFS_DIR/sbin/lfsmaster"
}

build_lizardfs() {
	build_lizardfs_or_use_cache
	test_lizardfs
}

lizardfs_chunkserver_daemon() {
	"$LIZARDFS_DIR/sbin/lfschunkserver" -c "${lizardfs_info_[chunkserver$1_config]}" "$2" | cat
	return ${PIPESTATUS[0]}
}

lizardfs_master_daemon() {
	"$LIZARDFS_DIR/sbin/lfsmaster" -c "${lizardfs_info_[master_cfg]}" "$1" | cat
	return ${PIPESTATUS[0]}
}

# A generic function to run LizardFS commands. Usage examples:
# lfs lfssetgoal 3 file
# lfs lfsdirinfo file
# lfs lfsmetalogger stop
lfs() {
	local command="$1"
	shift
	"$LIZARDFS_DIR/"*bin"/$command" "$@" | cat
	return ${PIPESTATUS[0]}
}
