cat >"$TEMP_DIR/test.cc" <<'END_OF_SOURCE'
#!/usr/bin/env cpp-interpreter.sh
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>
#define verifyThat(op) if (!(op)) { perror("failed: " #op) ; exit(0); }

void verifySize(int fd, unsigned expectedSize) {
	struct stat st;
	verifyThat(fstat(fd, &st) == 0);
	if (st.st_size != expectedSize) {
		fprintf(stderr, "Expected file size %u, got %u\n", expectedSize, unsigned(st.st_size));
		exit(0);
	}
}

int main(int argc, char** argv) {
	int fd = open(argv[1], O_RDWR | O_CREAT | O_TRUNC, 0644);
	verifyThat(fd >= 0);
	char block[10000];
	// Verify if we report proper size after each successful write
	for (unsigned i = 1; i <= 1000; ++i) {
		verifyThat(write(fd, block, sizeof(block)) == sizeof(block));
		verifySize(fd, i * sizeof(block));
	}
	// At the end truncate the file down and check if we report proper size after this operation
	verifyThat(ftruncate(fd, 12345) == 0);
	verifySize(fd, 12345);
	verifyThat(close(fd) == 0);
	return 0;
}
END_OF_SOURCE
chmod +x "$TEMP_DIR/test.cc"

USE_RAMDISK=YES setup_local_empty_lizardfs info
for i in {1..10}; do    # Run the test binary 10 times to detect races with a higher probability
	export MESSAGE="Veryfing values returned by getattr on file$i"
	assert_empty "$("$TEMP_DIR/test.cc" "${info[mount0]}/file$i" 2>&1)"
done
