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

#pragma once

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
	typedef LizardClient::JobId JobId;
	typedef LizardClient::AttrReply AttrReply;
	typedef LizardClient::DirEntry DirEntry;
	typedef LizardClient::EntryParam EntryParam;
	typedef LizardClient::Context Context;
	typedef std::vector<DirEntry> ReadDirReply;
	typedef ReadCache::Result ReadResult;

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

	/*! \brief Open a directory by inode */
	FileInfo *opendir(const Context &ctx, Inode ino);
	FileInfo *opendir(const Context &ctx, Inode ino, std::error_code &ec);

	/*! \brief Release a previously open directory */
	void releasedir(const Context &ctx, FileInfo* fileinfo);
	void releasedir(const Context &ctx, FileInfo* fileinfo, std::error_code &ec);

	/*! \brief Remove a directory */
	void rmdir(const Context &ctx, Inode parent, const std::string &path);
	void rmdir(const Context &ctx, Inode parent, const std::string &path, std::error_code &ec);

	/*! \brief Read directory contents */
	ReadDirReply readdir(const Context &ctx, FileInfo* fileinfo, off_t offset, size_t max_entries);
	ReadDirReply readdir(const Context &ctx, FileInfo* fileinfo, off_t offset, size_t max_entries, std::error_code &ec);

	/*! \brief Create a directory */
	void mkdir(const Context &ctx, Inode parent, const std::string &path, mode_t mode, EntryParam &entry_param);
	void mkdir(const Context &ctx, Inode parent, const std::string &path, mode_t mode, EntryParam &entry_param, std::error_code &ec);

	/*! \brief Read bytes from open file, returns read cache result that holds cache lock */
	ReadResult read(const Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size);
	ReadResult read(const Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size, std::error_code &ec);

	/*! \brief Write bytes to open file */
	std::size_t write(const Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size, const char *buffer);
	std::size_t write(const Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size, const char *buffer, std::error_code &ec);

	/*! \brief Release a previously open file */
	void release(const Context &ctx, FileInfo *fileinfo);
	void release(const Context &ctx, FileInfo *fileinfo, std::error_code &ec);

	/*! \brief Flush data written to an open file */
	void flush(const Context &ctx, FileInfo *fileinfo);
	void flush(const Context &ctx, FileInfo *fileinfo, std::error_code &ec);

	/*! \brief Get attributes by inode */
	void getattr(const Context &ctx, Inode ino, AttrReply &attr_reply);
	void getattr(const Context &ctx, Inode ino, AttrReply &attr_reply, std::error_code &ec);

	/*! \brief Create a snapshot of a file */
	JobId makesnapshot(const Context &ctx, Inode src_inode, Inode dst_inode,
	                  const std::string &dst_name, bool can_overwrite);
	JobId makesnapshot(const Context &ctx, Inode src_inode, Inode dst_inode,
	                  const std::string &dst_name, bool can_overwrite, std::error_code &ec);

	/*! \brief Get replication goal for a file */
	std::string getgoal(const Context &ctx, Inode ino);
	std::string getgoal(const Context &ctx, Inode ino, std::error_code &ec);

	/*! \brief Set replication goal for a file */
	void setgoal(const Context &ctx, Inode inode, const std::string &goal_name, uint8_t smode);
	void setgoal(const Context &ctx, Inode inode, const std::string &goal_name, uint8_t smode, std::error_code &ec);

protected:
	/*! \brief Initialize client with master host, port and mountpoint name
	 * \param host - master server connection address
	 * \param port - master server connection port
	 * \param mountpoint - human-readable 'mountpoint' name for web/cli interface
	 */
	int init(const std::string &host, const std::string &port, const std::string &mountpoint);

	void *linkLibrary();

	typedef decltype(&lzfs_disable_printf) DisablePrintfFunction;
	typedef decltype(&lizardfs_fs_init) FsInitFunction;
	typedef decltype(&lizardfs_fs_term) FsTermFunction;
	typedef decltype(&lizardfs_lookup) LookupFunction;
	typedef decltype(&lizardfs_mknod) MknodFunction;
	typedef decltype(&lizardfs_mkdir) MkDirFunction;
	typedef decltype(&lizardfs_rmdir) RmDirFunction;
	typedef decltype(&lizardfs_readdir) ReadDirFunction;
	typedef decltype(&lizardfs_opendir) OpenDirFunction;
	typedef decltype(&lizardfs_releasedir) ReleaseDirFunction;
	typedef decltype(&lizardfs_open) OpenFunction;
	typedef decltype(&lizardfs_getattr) GetattrFunction;
	typedef decltype(&lizardfs_read) ReadFunction;
	typedef decltype(&lizardfs_read_special_inode) ReadSpecialInodeFunction;
	typedef decltype(&lizardfs_write) WriteFunction;
	typedef decltype(&lizardfs_release) ReleaseFunction;
	typedef decltype(&lizardfs_flush) FlushFunction;
	typedef decltype(&lizardfs_isSpecialInode) IsSpecialInodeFunction;
	typedef decltype(&lizardfs_update_groups) UpdateGroupsFunction;
	typedef decltype(&lizardfs_makesnapshot) MakesnapshotFunction;
	typedef decltype(&lizardfs_getgoal) GetGoalFunction;
	typedef decltype(&lizardfs_setgoal) SetGoalFunction;

	DisablePrintfFunction lzfs_disable_printf_;
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
	ReadDirFunction lizardfs_readdir_;
	OpenDirFunction lizardfs_opendir_;
	ReleaseDirFunction lizardfs_releasedir_;
	RmDirFunction lizardfs_rmdir_;
	MkDirFunction lizardfs_mkdir_;
	MakesnapshotFunction lizardfs_makesnapshot_;
	GetGoalFunction lizardfs_getgoal_;
	SetGoalFunction lizardfs_setgoal_;

	void *dl_handle_;
	FileInfoList fileinfos_;
	std::mutex mutex_;

	static std::atomic<int> instance_count_;

	static constexpr const char *kLibraryPath = LIB_PATH "/liblizardfsmount_shared.so";
};

} // namespace lizardfs
