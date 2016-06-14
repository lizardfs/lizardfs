CHUNKSERVERS=2 \
	CHUNKSERVER_LABELS="0:A" \
	MASTER_EXTRA_CONFIG="CHUNKS_LOOP_MIN_TIME = 1" \
	MOUNT_EXTRA_CONFIG="mfscachemode=NEVER" \
	MASTER_CUSTOM_GOALS="5 AA: A A|6 A_: A _|7 A__: A _ _|8 BB: B B|`
			`9 B_: B _|10 BB_: B B _|11 AB: A B|12 AB_: A B _|`
			`13 AA_: A A _|14 B__: B _ _" \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

# Create one chunk in each of three goals
goals="2 3 4"
cd "${info[mount0]}"
for goal in $goals; do
	touch file_$goal
	lizardfs setgoal $goal file_$goal
	echo a > file_$goal
done

function chunks-health-trimmed() {
	lizardfs-probe chunks-health --porcelain localhost "${info[matocl]}" \
		| egrep -o "^... [234AB_]+ (0 )*[1-9]+" \
		| tr "\n" "|" \
		| sed 's/|$//'
}
# Check if the probe outputs proper values
first_output="AVA 2 1|AVA 3 1|AVA 4 1|REP 2 1|REP 3 0 1|REP 4 0 0 1|DEL 2 1|DEL 3 1|DEL 4 1"
expect_equals "$first_output" "$(chunks-health-trimmed)"

declare -A output
output+=([AA]="AVA AA 3|REP AA 0 3|DEL AA 0 3")
output+=([A_]="AVA A_ 3|REP A_ 3|DEL A_ 3")
output+=([AB]="AVA AB 3|REP AB 0 3|DEL AB 0 3")
output+=([BB]="AVA BB 3|REP BB 0 0 3|DEL BB 0 0 3")
output+=([B_]="AVA B_ 3|REP B_ 0 3|DEL B_ 0 3")
output+=([A__]="AVA A__ 3|REP A__ 0 3|DEL A__ 3")
output+=([AA_]="AVA AA_ 3|REP AA_ 0 3|DEL AA_ 3")
output+=([AB_]="AVA AB_ 3|REP AB_ 0 3|DEL AB_ 3")
output+=([BB_]="AVA BB_ 3|REP BB_ 0 0 3|DEL BB_ 0 3")
output+=([B__]="AVA B__ 3|REP B__ 0 3|DEL B__ 3")

# Check if the probe outputs proper values when we change
# files goal back and forth
for new_goal in "${!output[@]}"; do
	MESSAGE="Testing goal ${new_goal}"
	lizardfs setgoal ${new_goal} file_*
	expect_equals "${output[$new_goal]}" "$(chunks-health-trimmed)"
	chunks-health-trimmed
	for old_goal in $goals; do
		lizardfs setgoal ${old_goal} file_${old_goal}
	done
	expect_equals "$first_output" "$(chunks-health-trimmed)"
done

# Check if the output is fine after chunkserver disconnection and reconnection
output_without_chunkserver_A=`
	`"AVA 2 0 1|AVA 3 0 1|AVA 4 0 1|REP 2 0 1|REP 3 0 0 1|REP 4 0 0 0 1|DEL 2 1|DEL 3 1|DEL 4 1"
lizardfs_chunkserver_daemon 0 stop
assert_not_equal "$first_output" "$output_without_chunkserver_A"
assert_eventually_prints "$output_without_chunkserver_A" "chunks-health-trimmed"
lizardfs_chunkserver_daemon 0 start
assert_eventually_prints "$first_output" "chunks-health-trimmed"

