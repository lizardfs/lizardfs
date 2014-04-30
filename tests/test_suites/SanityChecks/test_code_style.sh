verify_file() {
	local file="$1"
	if grep $'[\t ]$' "$file"; then
		test_add_failure "File '$file' contains trailing whitespace"
	fi
	if grep $'[^\t]\t[^\t]' "$file"; then
		test_add_failure "File '$file' contains tabs which are not used for indentation"
	fi
	if [[ $(tail -c1 "$file" | wc -l) != 1 ]]; then
		test_add_failure "File '$file' doesn't end with LF"
	fi
	if [[ $(tail -c3 "$file" | wc -l) == 3 ]]; then
		test_add_failure "File '$file' ends with more then one blank line"
	fi
	if [[ $file =~ [.](cc|c|h|sh|inc)$ ]] && grep $'^    ' "$file"; then
		test_add_failure "File '$file' has lines indented with spaces"
	fi
	if [[ $file =~ src/.*[.](cc|h)$ ]] && ! grep -q '^# *include "config.h"' "$file"; then
		test_add_failure "File '$file' does not include config.h"
	fi
}

cd "$SOURCE_DIR"
if ! git rev-parse; then
	echo "'$SOURCE_DIR' is not a git repo or git not installed -- code style will not be checked!"
	test_end
fi
git ls-tree -r --name-only HEAD \
		| egrep '[.](cmake|txt|cc|c|h|sh|inc|in)$' \
		| grep -v 'mfs[.]cgi[.]in' \
		| while read file; do verify_file "$file"; done
