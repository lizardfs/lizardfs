set -eu

# Enable alias expansion and clear inherited aliases.
unalias -a
shopt -s expand_aliases extdebug

command_prefix=
for i in mfsmaster mfschunkserver mfsmount mfsmetarestore mfsmetalogger; do
	alias $i="\${command_prefix} $i"
done

. tools/config.sh # This has to be the first one
. tools/stack_trace.sh
. tools/assert.sh
. tools/lizardfs.sh
. tools/network.sh
. tools/random.sh
. tools/system.sh
. tools/test.sh
. tools/timeout.sh
. tools/valgrind.sh
. tools/time.sh
