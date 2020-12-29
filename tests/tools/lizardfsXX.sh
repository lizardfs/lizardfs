# Legacy version of LizardFS used in tests and sources of its packages
LIZARDFSXX_TAG="3.12.0"
LIZARDFSXX_PKG_FILE="/home/trzysiek/lizardfs-3.12.0.tar.gz"

install_lizardfsXX() {
	rm -rf "$LIZARDFSXX_DIR"
	mkdir -p "$LIZARDFSXX_DIR"
	tar -xf $LIZARDFSXX_PKG_FILE --directory $LIZARDFSXX_DIR_BASE
	cd $LIZARDFSXX_DIR_BASE
	for pkg in *.deb; do
		echo "Installing $pkg"
		dpkg -x $pkg install
	done
	echo "Legacy LizardFS packages installed."
	test_lizardfsXX_executables
}

test_lizardfsXX_executables() {
	test -x "$LIZARDFSXX_DIR/bin/mfsmount"
	test -x "$LIZARDFSXX_DIR/sbin/mfschunkserver"
	test -x "$LIZARDFSXX_DIR/sbin/mfsmaster"
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
