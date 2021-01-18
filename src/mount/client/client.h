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
#include "common/richacl.h"

#include <boost/intrusive/list.hpp>
#include <mutex>

namespace lizardfs {

/*!
 * \brief An object-based wrapper for LizardClient namespace.
 *
 * Dynamic library hacks are required, because LizardClient namespace is designed to be a singleton.
 */

class Client {
public:
	typedef LizardClient::FsInitParams FsInitParams;
	typedef LizardClient::Inode Inode;
	typedef LizardClient::JobId JobId;
	typedef LizardClient::NamedInodeOffset NamedInodeOffset;
	typedef LizardClient::AttrReply AttrReply;
	typedef std::vector<uint8_t> XattrBuffer;
	typedef LizardClient::DirEntry DirEntry;
	typedef LizardClient::EntryParam EntryParam;
	typedef LizardClient::Context Context;
	typedef std::vector<DirEntry> ReadDirReply;
	typedef ReadCache::Result ReadResult;
	typedef std::vector<NamedInodeEntry> ReadReservedReply;
	typedef std::vector<NamedInodeEntry> ReadTrashReply;
	typedef lzfs_locks::FlockWrapper FlockWrapper;

	struct Stats {
		uint64_t total_space;
		uint64_t avail_space;
		uint64_t trash_space;
		uint64_t reserved_space;
		uint32_t inodes;
	};

	struct FileInfo : public LizardClient::FileInfo, public boost::intrusive::list_base_hook<> {
		FileInfo() {}
		FileInfo(Inode inode, uint64_t opendirSessionID = 0)
			: inode(inode)
			, opendirSessionID(opendirSessionID) {
		}
		Inode inode;
		uint64_t opendirSessionID;
	};
	typedef boost::intrusive::list<FileInfo> FileInfoList;

	Client(const std::string &host, const std::string &port, const std::string &mountpoint);
	Client(FsInitParams &params);

	~Client();

	/*! \brief Update groups information */
	void updateGroups(Context &ctx);
	void updateGroups(Context &ctx, std::error_code &ec);

	/*! \brief Find inode in parent directory by name */
	void lookup(Context &ctx, Inode parent, const std::string &path, EntryParam &param);
	void lookup(Context &ctx, Inode parent, const std::string &path, EntryParam &param,
	            std::error_code &ec);

	/*! \brief Create a file with given parent and name */
	void mknod(Context &ctx, Inode parent, const std::string &path, mode_t mode,
	           dev_t rdev, EntryParam &param);
	void mknod(Context &ctx, Inode parent, const std::string &path, mode_t mode,
	           dev_t rdev, EntryParam &param, std::error_code &ec);

	/*! \brief Create a link with a given parent and name */
	void link(Context &ctx, Inode inode, Inode parent,
	             const std::string &name, EntryParam &param);
	void link(Context &ctx, Inode inode, Inode parent,
	             const std::string &name, EntryParam &param, std::error_code &ec);

	/*! \brief Create a symbolic link with a given parent and name */
	void symlink(Context &ctx, const std::string &link, Inode parent,
	             const std::string &name, EntryParam &param);
	void symlink(Context &ctx, const std::string &link, Inode parent,
	             const std::string &name, EntryParam &param, std::error_code &ec);

	/*! \brief Open a file by inode */
	FileInfo *open(Context &ctx, Inode inode, int flags);
	FileInfo *open(Context &ctx, Inode inode, int flags, std::error_code &ec);

	/*! \brief Open a directory by inode */
	FileInfo *opendir(Context &ctx, Inode ino);
	FileInfo *opendir(Context &ctx, Inode ino, std::error_code &ec);

	/*! \brief Release a previously open directory */
	void releasedir(FileInfo* fileinfo);
	void releasedir(FileInfo* fileinfo, std::error_code &ec);

	/*! \brief Remove a directory */
	void rmdir(Context &ctx, Inode parent, const std::string &path);
	void rmdir(Context &ctx, Inode parent, const std::string &path, std::error_code &ec);

	/*! \brief Read directory contents */
	ReadDirReply readdir(Context &ctx, FileInfo* fileinfo, off_t offset,
	                     size_t max_entries);
	ReadDirReply readdir(Context &ctx, FileInfo* fileinfo, off_t offset,
	                     size_t max_entries, std::error_code &ec);

	/*! \brief Read link contents */
	std::string readlink(Context &ctx, Inode inode);
	std::string readlink(Context &ctx, Inode inode, std::error_code &ec);

	/*! \brief Read reserved contents */
	ReadReservedReply readreserved(Context &ctx, NamedInodeOffset offset, NamedInodeOffset max_entries);
	ReadReservedReply readreserved(Context &ctx, NamedInodeOffset offset, NamedInodeOffset max_entries, std::error_code &ec);

	/*! \brief Read trash contents */
	ReadTrashReply readtrash(Context &ctx, NamedInodeOffset offset, NamedInodeOffset max_entries);
	ReadTrashReply readtrash(Context &ctx, NamedInodeOffset offset, NamedInodeOffset max_entries, std::error_code &ec);

	/*! \brief Create a directory */
	void mkdir(Context &ctx, Inode parent, const std::string &path, mode_t mode,
	           EntryParam &entry_param);
	void mkdir(Context &ctx, Inode parent, const std::string &path, mode_t mode,
	           EntryParam &entry_param, std::error_code &ec);

	/*! \brief Unlink a file by parent and name entry */
	void unlink(Context &ctx, Inode parent, const std::string &path);
	void unlink(Context &ctx, Inode parent, const std::string &path, std::error_code &ec);

	/*! \brief Undelete file from trash */
	void undel(Context &ctx, Inode ino);
	void undel(Context &ctx, Inode ino, std::error_code &ec);

	/*! \brief Rename a file */
	void rename(Context &ctx, Inode parent, const std::string &path, Inode new_parent,
	            const std::string &new_path);
	void rename(Context &ctx, Inode parent, const std::string &path, Inode new_parent,
	            const std::string &new_path, std::error_code &ec);

	/*! \brief Set inode attributes */
	void setattr(Context &ctx, Inode ino, struct stat *stbuf, int to_set,
	             AttrReply &attr_reply);
	void setattr(Context &ctx, Inode ino, struct stat *stbuf, int to_set,
	             AttrReply &attr_reply, std::error_code &ec);

	/*! \brief Read bytes from open file, returns read cache result that holds cache lock */
	ReadResult read(Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size);
	ReadResult read(Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size,
	                std::error_code &ec);

	/*! \brief Write bytes to open file */
	std::size_t write(Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size,
	                  const char *buffer);
	std::size_t write(Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size,
	                  const char *buffer, std::error_code &ec);

	/*! \brief Release a previously open file */
	void release(FileInfo *fileinfo);
	void release(FileInfo *fileinfo, std::error_code &ec);

	/*! \brief Flush data written to an open file */
	void flush(Context &ctx, FileInfo *fileinfo);
	void flush(Context &ctx, FileInfo *fileinfo, std::error_code &ec);

	/*! \brief Get attributes by inode */
	void getattr(Context &ctx, Inode ino, AttrReply &attr_reply);
	void getattr(Context &ctx, Inode ino, AttrReply &attr_reply, std::error_code &ec);

	/*! \brief Create a snapshot of a file */
	JobId makesnapshot(Context &ctx, Inode src_inode, Inode dst_inode,
	                  const std::string &dst_name, bool can_overwrite);
	JobId makesnapshot(Context &ctx, Inode src_inode, Inode dst_inode,
	                  const std::string &dst_name, bool can_overwrite, std::error_code &ec);

	/*! \brief Get replication goal for a file */
	std::string getgoal(Context &ctx, Inode ino);
	std::string getgoal(Context &ctx, Inode ino, std::error_code &ec);

	/*! \brief Set replication goal for a file */
	void setgoal(Context &ctx, Inode inode, const std::string &goal_name, uint8_t smode);
	void setgoal(Context &ctx, Inode inode, const std::string &goal_name, uint8_t smode,
	             std::error_code &ec);

	void fsync(Context &ctx, FileInfo *fileinfo);
	void fsync(Context &ctx, FileInfo *fileinfo, std::error_code &ec);

	void statfs(Stats &stats);
	void statfs(Stats &stats, std::error_code &ec);

	void setxattr(Context &ctx, Inode ino, const std::string &name,
	              const XattrBuffer &value, int flags);
	void setxattr(Context &ctx, Inode ino, const std::string &name,
	              const XattrBuffer &value, int flags, std::error_code &ec);

	XattrBuffer getxattr(Context &ctx, Inode ino, const std::string &name);
	XattrBuffer getxattr(Context &ctx, Inode ino, const std::string &name,
	                     std::error_code &ec);

	XattrBuffer listxattr(Context &ctx, Inode ino);
	XattrBuffer listxattr(Context &ctx, Inode ino, std::error_code &ec);

	void removexattr(Context &ctx, Inode ino, const std::string &name);
	void removexattr(Context &ctx, Inode ino, const std::string &name, std::error_code &ec);

	void setacl(Context &ctx, Inode ino, const RichACL &acl);
	void setacl(Context &ctx, Inode ino, const RichACL &acl, std::error_code &ec);

	RichACL getacl(Context &ctx, Inode ino);
	RichACL getacl(Context &ctx, Inode ino, std::error_code &ec);

	static std::vector<std::string> toXattrList(const XattrBuffer &buffer);

	std::vector<ChunkWithAddressAndLabel> getchunksinfo(Context &ctx, Inode ino,
	                                      uint32_t chunk_index, uint32_t chunk_count);
	std::vector<ChunkWithAddressAndLabel> getchunksinfo(Context &ctx, Inode ino,
	                                      uint32_t chunk_index, uint32_t chunk_count,
	                                      std::error_code &ec);

	std::vector<ChunkserverListEntry> getchunkservers();
	std::vector<ChunkserverListEntry> getchunkservers(std::error_code &ec);

	void getlk(Context &ctx, Inode ino, FileInfo *fileinfo, FlockWrapper &lock);
	void getlk(Context &ctx, Inode ino, FileInfo *fileinfo, FlockWrapper &lock,
	           std::error_code &ec);
	void setlk(Context &ctx, Inode ino, FileInfo *fileinfo, FlockWrapper &lock,
	               std::function<int(const lzfs_locks::InterruptData &)> handler);
	void setlk(Context &ctx, Inode ino, FileInfo *fileinfo, FlockWrapper &lock,
	               std::function<int(const lzfs_locks::InterruptData &)> handler,
	               std::error_code &ec);
	void setlk_interrupt(const lzfs_locks::InterruptData &data);
	void setlk_interrupt(const lzfs_locks::InterruptData &data, std::error_code &ec);

protected:
	/*! \brief Initialize client with parameters */
	void init(FsInitParams &params);

	void *linkLibrary();

	typedef decltype(&lizardfs_fs_init) FsInitFunction;
	typedef decltype(&lizardfs_fs_term) FsTermFunction;
	typedef decltype(&lizardfs_lookup) LookupFunction;
	typedef decltype(&lizardfs_mknod) MknodFunction;
	typedef decltype(&lizardfs_link) LinkFunction;
	typedef decltype(&lizardfs_symlink) SymlinkFunction;
	typedef decltype(&lizardfs_mkdir) MkDirFunction;
	typedef decltype(&lizardfs_rmdir) RmDirFunction;
	typedef decltype(&lizardfs_readdir) ReadDirFunction;
	typedef decltype(&lizardfs_readlink) ReadLinkFunction;
	typedef decltype(&lizardfs_readreserved) ReadReservedFunction;
	typedef decltype(&lizardfs_readtrash) ReadTrashFunction;
	typedef decltype(&lizardfs_opendir) OpenDirFunction;
	typedef decltype(&lizardfs_releasedir) ReleaseDirFunction;
	typedef decltype(&lizardfs_unlink) UnlinkFunction;
	typedef decltype(&lizardfs_undel) UndelFunction;
	typedef decltype(&lizardfs_open) OpenFunction;
	typedef decltype(&lizardfs_setattr) SetAttrFunction;
	typedef decltype(&lizardfs_getattr) GetAttrFunction;
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
	typedef decltype(&lizardfs_fsync) FsyncFunction;
	typedef decltype(&lizardfs_rename) RenameFunction;
	typedef decltype(&lizardfs_statfs) StatfsFunction;
	typedef decltype(&lizardfs_setxattr) SetXattrFunction;
	typedef decltype(&lizardfs_getxattr) GetXattrFunction;
	typedef decltype(&lizardfs_listxattr) ListXattrFunction;
	typedef decltype(&lizardfs_removexattr) RemoveXattrFunction;
	typedef decltype(&lizardfs_getchunksinfo) GetChunksInfoFunction;
	typedef decltype(&lizardfs_getchunkservers) GetChunkserversFunction;
	typedef decltype(&lizardfs_getlk) GetlkFunction;
	typedef decltype(&lizardfs_setlk_send) SetlkSendFunction;
	typedef decltype(&lizardfs_setlk_recv) SetlkRecvFunction;
	typedef decltype(&lizardfs_setlk_interrupt) SetlkInterruptFunction;

	FsInitFunction lizardfs_fs_init_;
	FsTermFunction lizardfs_fs_term_;
	LookupFunction lizardfs_lookup_;
	MknodFunction lizardfs_mknod_;
	MkDirFunction lizardfs_mkdir_;
	LinkFunction lizardfs_link_;
	SymlinkFunction lizardfs_symlink_;
	RmDirFunction lizardfs_rmdir_;
	ReadDirFunction lizardfs_readdir_;
	ReadLinkFunction lizardfs_readlink_;
	ReadReservedFunction lizardfs_readreserved_;
	ReadTrashFunction lizardfs_readtrash_;
	OpenDirFunction lizardfs_opendir_;
	ReleaseDirFunction lizardfs_releasedir_;
	UnlinkFunction lizardfs_unlink_;
	UndelFunction lizardfs_undel_;
	OpenFunction lizardfs_open_;
	SetAttrFunction lizardfs_setattr_;
	GetAttrFunction lizardfs_getattr_;
	ReadFunction lizardfs_read_;
	ReadSpecialInodeFunction lizardfs_read_special_inode_;
	WriteFunction lizardfs_write_;
	ReleaseFunction lizardfs_release_;
	FlushFunction lizardfs_flush_;
	IsSpecialInodeFunction lizardfs_isSpecialInode_;
	UpdateGroupsFunction lizardfs_update_groups_;
	MakesnapshotFunction lizardfs_makesnapshot_;
	GetGoalFunction lizardfs_getgoal_;
	SetGoalFunction lizardfs_setgoal_;
	FsyncFunction lizardfs_fsync_;
	RenameFunction lizardfs_rename_;
	StatfsFunction lizardfs_statfs_;
	SetXattrFunction lizardfs_setxattr_;
	GetXattrFunction lizardfs_getxattr_;
	ListXattrFunction lizardfs_listxattr_;
	RemoveXattrFunction lizardfs_removexattr_;
	GetChunksInfoFunction lizardfs_getchunksinfo_;
	GetChunkserversFunction lizardfs_getchunkservers_;
	GetlkFunction lizardfs_getlk_;
	SetlkSendFunction lizardfs_setlk_send_;
	SetlkRecvFunction lizardfs_setlk_recv_;
	SetlkInterruptFunction lizardfs_setlk_interrupt_;

	void *dl_handle_;
	FileInfoList fileinfos_;
	std::mutex mutex_;
	std::atomic<uint64_t> nextOpendirSessionID_;

	static std::atomic<int> instance_count_;

	static constexpr int kMaxXattrRequestSize = 65536;
	static constexpr const char *kLibraryPath = LIB_PATH "/liblizardfsmount_shared.so";
};

} // namespace lizardfs
