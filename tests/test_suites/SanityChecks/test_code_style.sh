timeout_set 1 minute

verify_file() {
	local file="$1"
	local grep='grep -Hn --color=auto'
	if $grep $'[\t ]$' "$file"; then
		test_add_failure "File '$file' contains trailing whitespace"
	fi
	if $grep $'[^\t]\t[^\t]' "$file"; then
		test_add_failure "File '$file' contains tabs which are not used for indentation"
	fi
	if [[ $(tail -c1 "$file" | wc -l) != 1 ]]; then
		test_add_failure "File '$file' doesn't end with LF"
	fi
	if [[ $(tail -c3 "$file" | wc -l) == 3 ]]; then
		test_add_failure "File '$file' ends with more then one blank line"
	fi
	if [[ $file =~ [.](cc|c|h|sh|inc)$ ]] && $grep $'^    ' "$file"; then
		test_add_failure "File '$file' has lines indented with spaces"
	fi
	if [[ $file =~ src/.*[.](cc|h)$ ]] && [[ $file != src/common/platform.h ]] && ! $grep -q '^# *include "common/platform.h"' "$file"; then
		test_add_failure "File '$file' does not include common/platform.h"
	fi
	if [[ $file =~ [.](cc|h)$ ]] && $grep '( \| )' "$file"; then
		test_add_failure "File '$file' has spaces around parens"
	fi
}

cd "$SOURCE_DIR"
if ! git rev-parse; then
	echo "'$SOURCE_DIR' is not a git repo or git not installed -- code style will not be checked!"
	test_end
fi
git ls-tree -r --name-only HEAD \
		| egrep '[.](cmake|txt|cc|c|h|sh|inc|in|cfg)$' \
		| grep -v 'mfs[.]cgi[.]in' \
		| grep -v 'lizardfs_c_api[.]h' \
		| grep -v 'lizardfs_error_codes[.]h' \
		| grep -v '^external/' \
		| grep -v 'src/nfs-ganesha' \
		| while read file; do verify_file "$file"; done
