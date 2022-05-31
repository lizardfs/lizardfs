/**
 * Interleaving readdir and unlink calls test.
 *
 * This tests if readdir() lists all existing entries in a directory when
 * removing already listed entries during this iteration.
 * This scenario presents typical situation of removing all entries from
 * a directory.
 *
 * Usage:
 *  ./exe <filenum> <workspace-path>
 * where:
 * - <filenum> is the number of files to create and then iterate over remove
 * - <workspace-path> is a path for this test to work in (create and remove files)
 *
 * First, it creates a test directory under <workspace-path> with <filenum> empty
 * files within in. Then it tries to remove all these files in readdir-unlink loop.
 * Finally, it removes the should-be-empty test directory using rmdir().
 *
 * The test executable returns:
 * - 0 on success
 * - 1 on failure (removed less files than created)
 * - 2 on any other error from a filesystem operation
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
#include <unistd.h>
#include <cstdlib>
#include <iostream>
#include <system_error>

const std::size_t FILE_NUM = 1024;
const char *TEST_DIR_NAME = "test-dir";

std::size_t create_files(std::size_t filenum);
std::size_t unlink_files_in_readdir_loop();
bool remove_empty_dir(const char *dirname);

int main(int argc, char *argv[]) {
	std::size_t filenum = FILE_NUM;
	int dirfd;
	if (argc > 1) {
		filenum = std::atoi(argv[1]);
	}
	try {
		if (argc > 2) {
			dirfd = ::open(argv[2], O_PATH|O_DIRECTORY);
			if (dirfd == -1) {
				std::cerr << "Cannot open directory " << argv[2] << '\n';
				throw std::system_error(errno, std::generic_category());
			}
			if (::fchdir(dirfd)) {
				std::cerr << "Cannot change cwd to " << argv[2] << '\n';
				throw std::system_error(errno, std::generic_category());
			}
		} else {
			dirfd = ::open(".", O_PATH|O_DIRECTORY);
			if (dirfd == -1) {
				std::cerr << "Cannot open directory \".\"\n";
				throw std::system_error(errno, std::generic_category());
			}
		}
		std::cout << "Number of files to create: " << filenum << '\n';

		if (::mkdir(TEST_DIR_NAME, S_IRWXU)) {
			std::cerr << "Cannot create directory " << TEST_DIR_NAME << '\n';
			throw std::system_error(errno, std::generic_category());
		}
		if (::chdir(TEST_DIR_NAME)) {
			std::cerr << "Cannot change cwd to " << TEST_DIR_NAME << '\n';
			throw std::system_error(errno, std::generic_category());
		}

		auto files_created = create_files(filenum);
		auto files_unlinked = unlink_files_in_readdir_loop();

		if (::fchdir(dirfd)) {
			std::cerr << "Cannot change cwd to the previous one\n";
			throw std::system_error(errno, std::generic_category());
		}
		auto rmdir_success = remove_empty_dir(TEST_DIR_NAME);
		::close(dirfd);

		if (rmdir_success && files_created == files_unlinked) {
			std::cout << "Success: created and unlinked " << files_created << " files\n";
		} else {
			std::cout << "Failure: created " << files_created << " files but unlinked " << files_unlinked << '\n';
			return 1;
		}
	} catch (const std::system_error &ex) {
		std::cerr << "ERROR: " << ex.code() << " : " << ex.what() << '\n';
		return 2;
	}
	return 0;
}

std::size_t create_files(std::size_t filenum) {
	std::size_t created = 0;
	for (std::size_t i = 0; i < filenum; ++i) {
		auto filename = std::to_string(i);
		// create an empty file
		int fd = ::open(filename.c_str(), O_CREAT|O_EXCL|O_WRONLY, S_IRUSR|S_IWUSR);
		if (fd == -1) {
			std::cerr << "Cannot create file " << filename << '\n';
			throw std::system_error(errno, std::generic_category());
		}
		::close(fd);
		++created;
	}
	return created;
}

std::size_t unlink_files_in_readdir_loop() {
	DIR *dir = ::opendir(".");
	if (!dir) {
		std::cerr << "Cannot open dirstream\n";
		throw std::system_error(errno, std::generic_category());
	}

	std::size_t unlinked = 0;
	struct dirent *file_ent;
	errno = 0;
	while (true) {
		file_ent = ::readdir(dir);
		if (file_ent == nullptr) {
			if (errno) {
				std::cerr << "Readdir error\n";
				throw std::system_error(errno, std::generic_category());
			}
			break;
		}
		// our names do not start with a dot
		if (file_ent->d_name[0] != '.') {
			if (::unlink(file_ent->d_name)) {
				std::cerr << "Cannot delete file " << file_ent->d_name << '\n';
				throw std::system_error(errno, std::generic_category());
			}
			++unlinked;
		}
	}
	::closedir(dir);
	::sync();
	return unlinked;
}

bool remove_empty_dir(const char *dirname) {
	if (::rmdir(dirname)) {
		std::cerr << "Cannot remove directory " << dirname << '\n';
		if (errno == static_cast<int>(std::errc::directory_not_empty)) {
			std::cerr << "Directory is not empty (ENOTEMPTY)\n";
			return false;
		}
		throw std::system_error(errno, std::generic_category());
	}
	return true;
}
