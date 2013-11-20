get_ip_addr() {
	hostname -I | cut -f1 -d' '
}

# Usage: get_next_port_number out_variable
# This function has to modify global variable and cannot be run from a subshell
get_next_port_number() {
	((FIRST_PORT_TO_USE++))
	eval "$1"=$FIRST_PORT_TO_USE
}
