# Message coloring functions.

RED=1
GREEN=2
YELLOW=3
BLUE=4
MAGENTA=5
CYAN=6

# Turn on given color for following text output.
col_on() {
	tput setaf ${!1}
	tput bold
}

# Turn off all colors for further text messages.
col_off() {
	tput sgr0
}

# Output message with given color.
msg() {
	col_on ${1}
	shift
	echo "${@}"
	col_off
}

lizardfs_make_conf_for_master_dbg() {
	msg MAGENTA lizardfs_make_conf_for_master_dbg $*
	col_on GREEN
	local old_master=$(lizardfs_current_master_id)
	echo -n "old master $old_master: "
	lizardfs_master_n ${old_master} test | cat
	col_off
	lizardfs_make_conf_for_master "${@}"
	local new_master=$1
	col_on GREEN
	echo -n "new master $new_master: "
	lizardfs_master_n $new_master test | cat
	col_off
	msg YELLOW "Server $new_master is becoming a master instead of $old_master"
	cat "${info[master${new_master}_cfg]}"
}

lizardfs_make_conf_for_shadow_dbg() {
	msg MAGENTA lizardfs_make_conf_for_shadow_dbg $*
	lizardfs_make_conf_for_shadow "${@}"
	msg YELLOW "Server $1 is becoming a shadow"
	cat "${info[master${1}_cfg]}"
}

