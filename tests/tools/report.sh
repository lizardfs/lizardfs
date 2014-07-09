# Fancy reporting

report() {
	$(${@})
	if [ "${*}" = "${*/test//}" ] ; then
		msg MAGENTA "${*}"
	fi
	sleep .1
	if [ "${!#}" != "test" ] ; then
		report_masters
	fi
}

report_masters() {
	REPORT=""
	for ((msid_loc=0 ; msid_loc<${info[masterserver_count]}; ++msid_loc)); do
		if [ "${msid_loc}" = "$(lizardfs_current_master_id)" ] ; then
			PERSONALITY="m"
		else
			PERSONALITY="s"
		fi
		RESULT=$(lizardfs_master_n ${msid_loc} test |& cat)
		if [ "${RESULT}" = "${RESULT/ not //}" ] ; then
			REPORT="${REPORT}${PERSONALITY^}"
		else
			REPORT="${REPORT}${PERSONALITY}"
		fi
	done
	unset msid_loc
	msg CYAN "Master state: ${REPORT}"
}
