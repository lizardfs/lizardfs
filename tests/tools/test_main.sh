# Enable alias expansion and clear inherited aliases.
unalias -a
shopt -s expand_aliases extdebug

command_prefix=
for i in mfsmaster mfschunkserver mfsmount mfsmetarestore mfsmetalogger \
		lizardfs-polonaise-server; do
	alias $i="\${command_prefix} $i"
done

. tools/config.sh # This has to be the first one
. $(which set_lizardfs_constants.sh)
. tools/stack_trace.sh
. tools/assert.sh
. tools/moosefs.sh
. tools/string.sh
. tools/lizardfs.sh
. tools/lizardfsXX.sh
. tools/network.sh
. tools/permissions.sh
. tools/random.sh
. tools/system.sh
. tools/test.sh
. tools/timeout.sh # has to be sourced after assert.sh
. tools/valgrind.sh
. tools/time.sh
. tools/quota.sh
. tools/metadata.sh
. tools/color.sh
. tools/continuous_test.sh
. tools/logs.sh
