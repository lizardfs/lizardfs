/*
   Copyright 2013-2014 EditShare, 2013-2017 Skytechnology sp. z o.o.

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

#include <string>

#include "common/cwrap.h"
#include "common/exceptions.h"

/*! \brief Create and manage lock files.
 */
class Lockfile {
public:
	/*! \brief Stale lock file handling strategy.
	 */
	enum class StaleLock {
		kSwallow, /*!< Create lock despite of stale lock file. */
		kReject /*!< Bail out while trying to create lock with existing stale lock file. */
	};

	/*! \brief Create new lock file object for lock file with given name.
	 *
	 * \param name - name for new lock file.
	 */
	Lockfile(const std::string& name);
	virtual ~Lockfile();

	/*! \brief Create lock and lock lock file.
	 *
	 * Create new lock file in file system and lock it
	 * using given stale lock file handling strategy.
	 *
	 * \param  staleLock - stale lock file handling strategy.
	 * \throw LockfileException
	 * \throw FilesystemException
	 */
	void lock(StaleLock staleLock = StaleLock::kReject);

	/*! \brief Unlock lock file and remove it from file system.
	 *
	 * \throw FilesystemException
	 */
	void unlock();

	/*! \brief Tell if this lockfile is currently locked.
	 *
	 * \return True iff this locked is currently locked.
	 */
	bool isLocked() const;

	/*! \brief Tell if this lockfile has currently some message written.
	 *
	 * \return True iff this locked has currently some message written.
	 * \throw FilesystemException
	 */
	bool hasMessage() const;

	/*! \brief Erases any messages present in the lockfile.
	 *
	 * \throw FilesystemException
	 */
	void eraseMessage();

	/*! \brief Write some message to the lockfile.
	 *
	 * \param message - message to be written to the lockfile.
	 * \throw FilesystemException
	 */
	void writeMessage(const std::string& message);

private:
	std::string name_;
	FileDescriptor fd_;
};

/*! \brief Lockfile related exception.
 */
class LockfileException : public FilesystemException {
public:
	/*! \brief Reason why an exception was thrown.
	 */
	enum class Reason {
		kStaleLock, /*!< Stale lock file was encountered. */
		kAlreadyLocked /*!< Lockfile was already locked. */
	};

	/*! \brief Construct exception.
	 *
	 * \param message - custom exception message.
	 * \param reason - reason this exception was thrown.
	 */
	LockfileException(const std::string& message, Reason reason)
		: FilesystemException(message), reason_(reason) {
	}

	~LockfileException() noexcept {
	}

	/*! \brief Report reason why this exception was thrown.
	 *
	 * \return Reason this exception was thrown.
	 */
	Reason reason() const {
		return reason_;
	}

private:
	Reason reason_;
};

