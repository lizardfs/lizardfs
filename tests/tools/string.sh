# remove leading and trailing whitespaces
function trim {
	sed -e 's/^[ \t]*//' -e 's/[ \t]*$//'
}

# remove leading and trailing whitespaces and replace
# remaining multiple whitespaces with a single space
function trim_hard {
	trim | sed -e 's/[ \t]\+/ /g'
}

function version_compare_gte() {
   sort_result=$(echo -e "${1}\n${2}" | sort -t '.' -k 1,1 -k 2,2 -g | head -n1)
   [ "$2" = "$sort_result" ]
}
