#pragma once

#include "common/platform.h"

#include <memory>
#include <string>

class FileDescriptor {
public:
	FileDescriptor();
	FileDescriptor(int fd);
	FileDescriptor(const FileDescriptor&) = delete;
	FileDescriptor& operator=(const FileDescriptor&) = delete;
	virtual ~FileDescriptor();
	int get() const;
	void reset(int fd);
	void close();
	bool isOpened() const;

private:
	int fd_;
};

struct CFileCloser {
	void operator()(FILE*) const;
};

typedef std::unique_ptr<FILE, CFileCloser> cstream_t;

std::string errorString(int errNo);

namespace fs {

/*! \brief Check if given file exists.
 *
 * \param path - test existence of this file.
 * \return True iff given file does not exists.
 * \throw FilesystemException if the was a problem with obtaining files ontological status.
 */
bool exists(const std::string& path);

/*! \brief Change file path.
 *
 * \param currentPath - current path.
 * \param newPath - new path.
 * \throw FilesystemException if renaming file failed.
 */
void rename(const std::string& currentPath, const std::string& newPath);

/*! \brief Remove given file.
 *
 * \param path - path to file to be removed.
 * \throw FilesystemException if removal of given file failed.
 */
void remove(const std::string& path);

/*! \brief Get directory part of given file name.
 *
 * \param path - path to get directory part from.
 * \return Directory part of path.
 */
std::string dirname(const std::string& path);

/*! \brief Get current working directory.
 */
std::string getCurrentWorkingDirectory();

/*! \brief Get current working directory. No throw guarantee.
 */
std::string getCurrentWorkingDirectoryNoThrow();

}
