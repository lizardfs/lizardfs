# Functions to get additional logs from tests for debugging failures

save_oplog_to_file() {
	local mountdir=$1
	local oplog_output_file=$2
	sudo cat "${mountdir}/.oplog" > "${oplog_output_file}"
}
