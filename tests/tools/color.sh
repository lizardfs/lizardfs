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

