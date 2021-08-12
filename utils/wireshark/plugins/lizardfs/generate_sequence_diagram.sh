#!/usr/bin/env bash
me=$(basename -s .sh $0)

declare -A tokens
tokens[AN]=ANY
tokens[CL]=CLIENT
tokens[CS]=CHUNKSERVER
tokens[MA]=MASTER
tokens[ML]=METALOGGER
tokens[TS]=TAPESERVER
declare -a sorted_keys=(CL MA CS ML TS AN)

usage(){
	cat <<EOT
USAGE: ./${me}.sh path/to/MFSCommunication.h list_of_arguments

All arguments should be passed in a following way: ARG_NAME=value
List of arguments:
PARTICIPANTS   - optional filter, e.g., 'AN|CL|CS|MA|ML|TS'
ACTIONS        - optional filter, e.g., 'CLTOMA|GET_CHUNK|WRITE_END'
EOT
	exit 1
}

for ARGUMENT in "$@"; do
	KEY=$(echo $ARGUMENT | cut -f1 -d=)
	VALUE=$(echo $ARGUMENT | cut -f2 -d=)

	case "$KEY" in
		PARTICIPANTS)      PARTICIPANTS=${VALUE} ;;
		ACTIONS)           ACTIONS=${VALUE} ;;
		*) ;;
	esac
done

source_file="${1:-}"
[ -z "${source_file}" ] && usage

participant_filter="${PARTICIPANTS:-}"
action_filter="${ACTIONS:-}"

if [ -n "${participant_filter}" ]; then
	key_list="${!tokens[@]}"
	for key in ${key_list}; do
		echo "${participant_filter}" | grep -q '\b'"${key}"'\b'
		if [ "$?" != "0" ]; then
			unset tokens["${key}"]
		fi
	done
fi

print_header() {
	echo "title LizardFS Protocol"
	echo
	for key in "${sorted_keys[@]}"; do
		if [ -n "${tokens[$key]}" ]; then
			echo "participant ${tokens[$key]}"
		fi
	done
	echo
}

get_key_disjunction() {
	local key_list="${!tokens[@]}"
	echo "${key_list}" | tr ' ' '|'
}

get_search_pattern() {
	ored_keys="$(get_key_disjunction)"
	echo '('"${ored_keys}"')TO('"${ored_keys}"')'
}

get_transformation_pattern() {
	search_pattern="$(get_search_pattern)"
	local key_list="${!tokens[@]}"

	echo 's/(LIZ_)?'"${search_pattern}"'/\2->\3:\0/;'
	for key in ${key_list}; do
		echo 's/(^|>)'"${key}"'/\1'"${tokens[${key}]}"'/g;'
	done
}

extract_sequence() {
	search_pattern="$(get_search_pattern)"
	transform_pattern="$(get_transformation_pattern)"

	cat "${source_file}" \
		| egrep -o '#define\s+\w+' \
		| awk '{print $2}' \
		| egrep "${search_pattern}" \
		| sed -E "${transform_pattern}"
}

main() {
	print_header

	if [ -z "${action_filter}" ]; then
		extract_sequence
	else
		extract_sequence | egrep "${action_filter}"
	fi
}

main
