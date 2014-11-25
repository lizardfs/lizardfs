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

	/*! \brief Construct exeception.
	 *
	 * \param message - custom exception message.
	 * \param reason - reason this exception was thrown.
	 */
	LockfileException(const std::string& message, Reason reason)
		: FilesystemException(message), reason_(reason) {
	}

	~LockfileException() throw() {
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

