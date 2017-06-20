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

	/*! \brief Update groups information */
	void updateGroups(Context &ctx);
	void updateGroups(Context &ctx, std::error_code &ec);

	/*! \brief Find inode in parent directory by name */
	void lookup(const Context &ctx, Inode parent, const std::string &path, EntryParam &param);
	void lookup(const Context &ctx, Inode parent, const std::string &path, EntryParam &param, std::error_code &ec);

	/*! \brief Create a file with given parent and name */
	void mknod(const Context &ctx, Inode parent, const std::string &path, mode_t mode, EntryParam &param);
	void mknod(const Context &ctx, Inode parent, const std::string &path, mode_t mode, EntryParam &param, std::error_code &ec);

	/*! \brief Open a file by inode */
	FileInfo *open(const Context &ctx, Inode inode, int flags);
	FileInfo *open(const Context &ctx, Inode inode, int flags, std::error_code &ec);

	/*! \brief Read bytes from open file */
	std::size_t read(const Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size, char *buffer);
	std::size_t read(const Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size, char *buffer, std::error_code &ec);

	/*! \brief Write bytes to open file */
	std::size_t write(const Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size, const char *buffer);
	std::size_t write(const Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size, const char *buffer, std::error_code &ec);

	/*! \brief Release a previously open file */
	void release(const Context &ctx, FileInfo *fileinfo);
	void release(const Context &ctx, FileInfo *fileinfo, std::error_code &ec);

	/*! \brief Flush data written to an open file */
	void flush(const Context &ctx, FileInfo *fileinfo);
	void flush(const Context &ctx, FileInfo *fileinfo, std::error_code &ec);

	/*! \brief Get attributes from an open file */
	void getattr(const Context &ctx, FileInfo* fileinfo, AttrReply &attr_reply);
	void getattr(const Context &ctx, FileInfo* fileinfo, AttrReply &attr_reply, std::error_code &ec);

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
	typedef decltype(&lizardfs_update_groups) UpdateGroupsFunction;

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
	UpdateGroupsFunction lizardfs_update_groups_;

	void *dl_handle_;
	FileInfoList fileinfos_;
	std::mutex mutex_;

	static std::atomic<int> instance_count_;

	static constexpr const char *kLibraryPath = LIB_PATH "/liblizardfsmount_shared.so";
};

} // namespace lizardfs
