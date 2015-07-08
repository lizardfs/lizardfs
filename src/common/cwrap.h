/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/platform.h"

#include <dirent.h>
#include <sys/types.h>
#include <cstdio>
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

struct CDirCloser {
	void operator()(DIR*) const;
};

typedef std::unique_ptr<FILE, CFileCloser> cstream_t;
typedef std::unique_ptr<DIR, CDirCloser> cdirectory_t;

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
