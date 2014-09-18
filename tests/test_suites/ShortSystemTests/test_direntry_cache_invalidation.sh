cat >"$TEMP_DIR/test.cc" <<'END_OF_SOURCE'
#!/usr/bin/env cpp-interpreter.sh
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <string>

#define verifyThat(op) if (!(op)) { perror("failed: " #op) ; exit(0); }

int main(int argc, char** argv) {
	int fd;
	verifyThat(argc == 2);
	DIR* dh = opendir(".");                                    // open the current directory
	verifyThat(dh != NULL);
	struct dirent* d = NULL;
	while (d == NULL || d->d_name[0] == '.') {                 // find any file there
		verifyThat((d = readdir(dh)) != NULL);
	}
	std::string filename(d->d_name);                           // and remember its name
	verifyThat(rename(filename.c_str(), argv[1]) == 0);        // rename the file to argv[1]
	verifyThat(open(filename.c_str(), O_RDONLY, 0) == -1);     // shouldn't exist now!
	fd = open(filename.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0644); // create a new empty file
	verifyThat(fd > 0);
	verifyThat(close(fd) == 0);                                // close the file
	verifyThat(closedir(dh) == 0);                             // and close the directory
	fd = open(filename.c_str(), O_RDONLY, 0);                  // verify if our new file exists
	verifyThat(fd >= 0);
	verifyThat(close(fd) == 0);
	return 0;
}
END_OF_SOURCE
chmod +x "$TEMP_DIR/test.cc"

USE_RAMDISK=YES setup_local_empty_lizardfs info
cd "${info[mount0]}"
FILE_SIZE=1K file-generate file
assert_empty "$("$TEMP_DIR/test.cc" back 2>&1)" # rename 'file' to 'back', create a new empty 'file'
assert_file_exists back
assert_success file-validate back
assert_file_exists file
assert_equals 0 "$(stat -c %s file)"
