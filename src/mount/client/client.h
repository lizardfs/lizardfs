/*
   Copyright 2017 Skytechnology sp. z o.o..

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/platform.h"

#include "client/lizard_client_c_linkage.h"

#include <boost/intrusive/list.hpp>
#include <mutex>
#include <sys/uio.h>

namespace lizardfs {

/*!
 * \brief An object-based wrapper for LizardClient namespace.
 *
 * Dynamic library hacks are required, because LizardClient namespace is designed to be a singleton.
 */
class Client {
public:
	typedef LizardClient::Inode Inode;
	typedef LizardClient::AttrReply AttrReply;
	typedef LizardClient::EntryParam EntryParam;
	typedef LizardClient::Context Context;

	struct FileInfo : public LizardClient::FileInfo, public boost::intrusive::list_base_hook<> {
		FileInfo() {}
		FileInfo(Inode inode) : inode(inode) {}
		Inode inode;
	};
	typedef boost::intrusive::list<FileInfo> FileInfoList;

	Client(const std::string &host, const std::string &port, const std::string &mountpoint);

	~Client();

	/*! \brief Find inode in parent directory by name */
	int lookup(const Context &ctx, Inode parent, const std::string &path, EntryParam &param);

	/*! \brief Create a file with given parent and name */
	int mknod(const Context &ctx, Inode parent, const std::string &path, mode_t mode, EntryParam &param);

	/*! \brief Open a file by inode */
	FileInfo *open(const Context &ctx, Inode inode, int flags);

	/*! \brief Read bytes from open file */
	ssize_t read(const Context &ctx, FileInfo *fileinfo, off_t offset, int size, char *buffer);

	/*! \brief Write bytes to open file */
	ssize_t write(const Context &ctx, FileInfo *fileinfo, off_t offset, int size, const char *buffer);

	/*! \brief Release a previously open file */
	int release(const Context &ctx, FileInfo *fileinfo);

	/*! \brief Flush data written to an open file */
	int flush(const Context &ctx, FileInfo *fileinfo);

	/*! \brief Get attributes from an open file */
	int getattr(const Context &ctx, FileInfo* fileinfo, AttrReply &attr_reply);

protected:
	/*! \brief Initialize client with master host, port and mountpoint name
	 * \param host - master server connection address
	 * \param port - master server connection port
	 * \param mountpoint - human-readable 'mountpoint' name for web/cli interface
	 */
	int init(const std::string &host, const std::string &port, const std::string &mountpoint);

	void *linkLibrary();

	typedef decltype(&lizardfs_fs_init) FsInitFunction;
	typedef decltype(&lizardfs_fs_term) FsTermFunction;
	typedef decltype(&lizardfs_lookup) LookupFunction;
	typedef decltype(&lizardfs_mknod) MknodFunction;
	typedef decltype(&lizardfs_open) OpenFunction;
	typedef decltype(&lizardfs_getattr) GetattrFunction;
	typedef decltype(&lizardfs_read) ReadFunction;
	typedef decltype(&lizardfs_read_special_inode) ReadSpecialInodeFunction;
	typedef decltype(&lizardfs_write) WriteFunction;
	typedef decltype(&lizardfs_release) ReleaseFunction;
	typedef decltype(&lizardfs_flush) FlushFunction;
	typedef decltype(&lizardfs_isSpecialInode) IsSpecialInodeFunction;

	FsInitFunction lizardfs_fs_init_;
	FsTermFunction lizardfs_fs_term_;
	LookupFunction lizardfs_lookup_;
	MknodFunction lizardfs_mknod_;
	OpenFunction lizardfs_open_;
	GetattrFunction lizardfs_getattr_;
	ReadFunction lizardfs_read_;
	ReadSpecialInodeFunction lizardfs_read_special_inode_;
	WriteFunction lizardfs_write_;
	ReleaseFunction lizardfs_release_;
	FlushFunction lizardfs_flush_;
	IsSpecialInodeFunction lizardfs_isSpecialInode_;

	void *dl_handle_;
	FileInfoList fileinfos_;
	std::mutex mutex_;

	static std::atomic<int> instance_count_;

	static constexpr const char *kLibraryPath = LIB_PATH "/liblizardfsmount_shared.so";
};

} // namespace lizardfs
