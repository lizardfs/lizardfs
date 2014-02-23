#!/bin/sh

# Usage: test_mfsmaster_agent [-u <user>] [-g <group>] [<agent-script>]
#   <user> defaults to the current user, or mfs if the current user is root
#   <group> defaults to the primary group of <user>
#   <agent-script> defaults to the mfsmaster agent script in the current
#     directory
#
# Note: ocf-tester requires that this be run as root, despite the fact that
#   we just want to run as an ordinary user, so we just tell everything to
#   run as the current user (or specified user).

# Include our return code constants so tests for specific codes are more
# readable
export OCF_ROOT=/usr/lib/ocf/
. $OCF_ROOT/lib/heartbeat/ocf-returncodes

USER=${SUDO_USER:-$([ x"$USER" = xroot ] && echo mfs || echo "$USER")}
GROUP=$(id --group --name "$USER")

while getopts "u:g:" OPTION
do
	case $OPTION in
		u) USER=$OPTION ;;
		g) GROUP=$OPTION ;;
		*) exit 1 ;;
	esac
done

RESOURCE_AGENT=${1:-${PWD}/mfsmaster}

test_action() {
	action=$1
	export __OCF_ACTION=action
	${RESOURCE_AGENT} $action
}

setup_environment() {
	# Usage: setup_environment [<dirname>] [<host>]
	dirname=${1:-mfsmaster_agent_test}
	host=${2:-"*"}

	echo "setup_environment $dirname $host"

	export OCF_RESOURCE_INSTANCE=mfsmaster_test
	export OCF_RESKEY_config_dir=/tmp/$dirname/etc
	export OCF_RESKEY_data_dir=/tmp/$dirname/var
	export OCF_RESKEY_run_dir=/tmp/$dirname/run
	export OCF_RESKEY_user=$USER
	export OCF_RESKEY_group=$GROUP
	export OCF_RESKEY_matoml_host="$host"
	export OCF_RESKEY_matocs_host="$host"
	export OCF_RESKEY_matocu_host="$host"
}

setup_tests() {
	# Usage: setup_tests [<dirname>] [<host>]
	dirname=${1:-mfsmaster_agent_test}
	host=${2:-"*"}

	setup_environment "$dirname" "$host"

	mkdir -p $OCF_RESKEY_config_dir
	mkdir -p $OCF_RESKEY_data_dir
	mkdir -p $OCF_RESKEY_run_dir
	chown $USER:$GROUP $OCF_RESKEY_config_dir
	chown $USER:$GROUP $OCF_RESKEY_data_dir
	chown $USER:$GROUP $OCF_RESKEY_run_dir

	touch $OCF_RESKEY_config_dir/mfsmaster.cfg
	cat >$OCF_RESKEY_config_dir/mfsexports.cfg <<EOF
# Allow everything but "meta"
* / rw,alldirs,maproot=0
# Allow "meta"
* . rw
EOF
}

cleanup_tests() {
	# Usage: cleanup_tests [<dirname>] [<host>]
	dirname=${1:-mfsmaster_agent_test}
	host=${2:-"*"}

	setup_environment "$dirname" "$host"

	test_action stop || die "failed to clean up after tests"
	rm -r /tmp/"$dirname"
}

die() {
	echo "$@" 1>&2
	exit 1
}

test_basic_functionality() {
	# My own sanity check to ensure that the basics are working properly before
	# passing testing off to ocf-tester

	(
		setup_tests
		test_action monitor && die "mfsmaster already running"
		test_action start || die "failed to start mfsmaster"
		test_action start || die "starting started mfsmaster failed"
		test_action monitor || die "mfsmaster not running after started"
		test_action stop || die "failed to stop mfsmaster"
		test_action monitor && die "mfsmaster running after stopped"
		test_action stop || die "stopping stopped mfsmaster failed"
		cleanup_tests
	)
}

ocf_test() {
	# A generic test suite to test basic functionality of OCF resource agents.
	# Acts basically like the above sanity check, but calls promote, demote,
	# notify, meta-data, and so on as well, verifying that their behavior
	# conforms to the spec.  Does not actually tests multiple instances,
	# however.

	(
		setup_tests
		/usr/sbin/ocf-tester -v                    \
			-n mfsmaster-test                      \
			-o config_dir=$OCF_RESKEY_config_dir   \
			-o data_dir=$OCF_RESKEY_data_dir       \
			-o run_dir=$OCF_RESKEY_run_dir         \
			-o user=$OCF_RESKEY_user               \
			-o group=$OCF_RESKEY_group             \
			${RESOURCE_AGENT} || die "failed ocf-tester suite"
		cleanup_tests
	)
}

multi_test() {
	(
		set -e
		trap "cleanup_tests test_host_1 127.0.42.1;
			  cleanup_tests test_host_2 127.0.42.2;" EXIT

		# Test two servers, each listening on their own localhost address
		(
			echo "Starting test_host_1"
			setup_tests test_host_1 127.0.42.1
			test_action monitor && die "mfsmaster already running"
			test_action start || die "failed to start mfsmaster"
		) || die "failed to start test_host_1"

		(
			echo "Starting test_host_2"
			setup_tests test_host_2 127.0.42.2
			test_action monitor && die "mfsmaster already running"
			test_action start || die "failed to start mfsmaster"

			echo "Notifying test_host_2 of test_host_1 promotion"
			# Now notify this one that the other is about to become a master;
			# the pre-promote notification is sent before the actual promotion
			# happens
			export OCF_RESKEY_CRM_meta_notify_type=pre
			export OCF_RESKEY_CRM_meta_notify_operation=promote
			export OCF_RESKEY_CRM_meta_notify_promote_uname=127.0.42.1
			test_action notify \
				|| die "failed to notify shadow master of new master"
		) || die "failed to start & notify test_host_2"

		(
			echo "Promoting test_host_1 to master"
			setup_environment test_host_1 127.0.42.1
			test_action promote || die "failed to promote to master"
			test_action monitor
			ret=$?
			if [ $ret -ne $OCF_RUNNING_MASTER ]
			then
				die "isn't running in master mode: $ret"
			fi
		) || die "failed to promote test_host_1"

		(
			echo "Probing test_host_2"
			setup_environment test_host_2 127.0.42.2
			test_action monitor
			ret=$?
			if [ $ret -ne $OCF_SUCCESS ]
			then
				die "isn't running in shadow mode: $ret"
			fi
		) || die "failed to probe test_host_2"

		(
			echo "Demoting test_host_1"
			setup_environment test_host_1 127.0.42.1
			test_action demote || die "failed to demote master"
			test_action monitor
			ret=$?
			if [ $ret -ne $OCF_SUCCESS ]
			then
				die "isn't running in shadow mode: $ret"
			fi
		) || die "failed to demote test_host_1"

		(
			echo "Notifying test_host_2 of demotion"
			setup_environment test_host_2 127.0.42.2
			export OCF_RESKEY_CRM_meta_notify_type=post
			export OCF_RESKEY_CRM_meta_notify_operation=demote
			export OCF_RESKEY_CRM_meta_notify_demote_uname=127.0.42.1
			test_action notify \
				|| die "failed to notify shadow master of demotion"
		) || die "failed to notify shadow master of demotion"

		(
			echo "Notifying test_host_1 of pending promotion"
			setup_environment test_host_1 127.0.42.1
			export OCF_RESKEY_CRM_meta_notify_type=pre
			export OCF_RESKEY_CRM_meta_notify_operation=promote
			export OCF_RESKEY_CRM_meta_notify_promote_uname=127.0.42.2
			test_action notify \
				|| die "failed to notify shadow master of new master"
		) || die "failed to notify shadow master ofnew master"

		(
			echo "Promoting test_host_2 to master"
			setup_environment test_host_2 127.0.42.2
			test_action promote || die "failed to promote to master"
			test_action monitor
			ret=$?
			if [ $ret -ne $OCF_RUNNING_MASTER ]
			then
				die "isn't running in master mode: $ret"
			fi
		) || die "failed to promote test_host_2"

		(
			echo "Probing test_host_1"
			setup_environment test_host_1 127.0.42.1
			test_action monitor
			ret=$?
			if [ $ret -ne $OCF_SUCCESS ]
			then
				die "isn't running in shadow mode: $?"
			fi
		) || die "failed to probe test_host_1"

		echo "Tests all passed, should clean up"
	)
}

test_basic_functionality || exit 1
ocf_test || exit 1
multi_test || exit 1
