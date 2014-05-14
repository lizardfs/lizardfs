# remove leading and trailing whitespaces
function trim {
	sed -e 's/^[ \t]*//' -e 's/[ \t]*$//'
}

# remove leading and trailing whitespaces and replace
# remaining multiple whitespaces with a single space
function trim_hard {
	trim | sed -e 's/[ \t]\+/ /g'
}
