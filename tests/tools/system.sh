# Clears page cache
drop_caches() {
	sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches'
}

is_program_installed() {
	return $(which "$1" &>/dev/null)
}

system_init() {
	ulimit -n 10000
}
