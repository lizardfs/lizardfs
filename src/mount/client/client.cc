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

#include "client.h"

#include <dlfcn.h>
#include <fstream>

#include "client_error_code.h"
#include "common/richacl_converter.h"

#define LIZARDFS_LINK_FUNCTION(function_name) do { \
	void *function_name##_ptr = dlsym(dl_handle_, #function_name); \
	function_name##_ = *(decltype(function_name##_)*)&function_name##_ptr; \
	if (function_name##_ == nullptr) { \
		throw std::runtime_error(std::string("dl lookup failed for ") + #function_name); \
	} \
} while (0)

using namespace lizardfs;

static const char *kRichAclXattrName = "system.richacl";

std::atomic<int> Client::instance_count_(0);

void *Client::linkLibrary() {
	void *ret;

	// Special case for the first instance - no copying needed
	if (instance_count_++ == 0) {
		ret = dlopen(kLibraryPath, RTLD_NOW);
		if (ret == nullptr) {
			instance_count_--;
			throw std::runtime_error(std::string("Cannot link: ") + dlerror());
		}
		return ret;
	}

	char pattern[] = "/tmp/liblizardfsmount_shared.so.XXXXXX";
	int out_fd = ::mkstemp(pattern);
	if (out_fd < 0) {
		instance_count_--;
		throw std::runtime_error("Cannot create temporary file");
	}

	std::ifstream source(kLibraryPath);
	std::ofstream dest(pattern);

	dest << source.rdbuf();

	source.close();
	dest.close();
	ret = dlopen(pattern, RTLD_NOW);
	::close(out_fd);
	::unlink(pattern);
	if (ret == nullptr) {
		instance_count_--;
		throw std::runtime_error(std::string("Cannot link: ") + dlerror());
	}
	return ret;
}

Client::Client(const std::string &host, const std::string &port, const std::string &mountpoint)
	: nextOpendirSessionID_(1) {
	FsInitParams params("", host, port, mountpoint);
	init(params);
}

Client::Client(FsInitParams &params)
	: nextOpendirSessionID_(1) {
	init(params);
}

Client::~Client() {
	assert(instance_count_ >= 1);
	assert(dl_handle_);

	while (!fileinfos_.empty()) {
		release(std::addressof(fileinfos_.front()));
	}

	lizardfs_fs_term_();
	dlclose(dl_handle_);
	instance_count_--;
}

void Client::init(FsInitParams &params) {
	dl_handle_ = linkLibrary();
	try {
		LIZARDFS_LINK_FUNCTION(lizardfs_fs_init);
		LIZARDFS_LINK_FUNCTION(lizardfs_fs_term);
		LIZARDFS_LINK_FUNCTION(lizardfs_lookup);
		LIZARDFS_LINK_FUNCTION(lizardfs_mknod);
		LIZARDFS_LINK_FUNCTION(lizardfs_link);
		LIZARDFS_LINK_FUNCTION(lizardfs_symlink);
		LIZARDFS_LINK_FUNCTION(lizardfs_mkdir);
		LIZARDFS_LINK_FUNCTION(lizardfs_rmdir);
		LIZARDFS_LINK_FUNCTION(lizardfs_readdir);
		LIZARDFS_LINK_FUNCTION(lizardfs_readlink);
		LIZARDFS_LINK_FUNCTION(lizardfs_readreserved);
		LIZARDFS_LINK_FUNCTION(lizardfs_readtrash);
		LIZARDFS_LINK_FUNCTION(lizardfs_opendir);
		LIZARDFS_LINK_FUNCTION(lizardfs_releasedir);
		LIZARDFS_LINK_FUNCTION(lizardfs_unlink);
		LIZARDFS_LINK_FUNCTION(lizardfs_undel);
		LIZARDFS_LINK_FUNCTION(lizardfs_open);
		LIZARDFS_LINK_FUNCTION(lizardfs_setattr);
		LIZARDFS_LINK_FUNCTION(lizardfs_getattr);
		LIZARDFS_LINK_FUNCTION(lizardfs_read);
		LIZARDFS_LINK_FUNCTION(lizardfs_read_special_inode);
		LIZARDFS_LINK_FUNCTION(lizardfs_write);
		LIZARDFS_LINK_FUNCTION(lizardfs_release);
		LIZARDFS_LINK_FUNCTION(lizardfs_flush);
		LIZARDFS_LINK_FUNCTION(lizardfs_isSpecialInode);
		LIZARDFS_LINK_FUNCTION(lizardfs_update_groups);
		LIZARDFS_LINK_FUNCTION(lizardfs_makesnapshot);
		LIZARDFS_LINK_FUNCTION(lizardfs_getgoal);
		LIZARDFS_LINK_FUNCTION(lizardfs_setgoal);
		LIZARDFS_LINK_FUNCTION(lizardfs_fsync);
		LIZARDFS_LINK_FUNCTION(lizardfs_rename);
		LIZARDFS_LINK_FUNCTION(lizardfs_statfs);
		LIZARDFS_LINK_FUNCTION(lizardfs_setxattr);
		LIZARDFS_LINK_FUNCTION(lizardfs_getxattr);
		LIZARDFS_LINK_FUNCTION(lizardfs_listxattr);
		LIZARDFS_LINK_FUNCTION(lizardfs_removexattr);
		LIZARDFS_LINK_FUNCTION(lizardfs_getchunksinfo);
		LIZARDFS_LINK_FUNCTION(lizardfs_getchunkservers);
		LIZARDFS_LINK_FUNCTION(lizardfs_getlk);
		LIZARDFS_LINK_FUNCTION(lizardfs_setlk_send);
		LIZARDFS_LINK_FUNCTION(lizardfs_setlk_recv);
		LIZARDFS_LINK_FUNCTION(lizardfs_setlk_interrupt);
	} catch (const std::runtime_error &e) {
		dlclose(dl_handle_);
		instance_count_--;
		throw e;
	}

	if (lizardfs_fs_init_(params) != 0) {
		assert(dl_handle_);
		dlclose(dl_handle_);
		instance_count_--;
		throw std::runtime_error("Can't connect to master server");
	}
}

void Client::updateGroups(Context &ctx) {
	std::error_code ec;
	updateGroups(ctx, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::updateGroups(Context &ctx, std::error_code &ec) {
	auto ret = lizardfs_update_groups_(ctx);
	ec = make_error_code(ret);
}


void Client::lookup(Context &ctx, Inode parent, const std::string &path, EntryParam &param) {
	std::error_code ec;
	lookup(ctx, parent, path, param, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::lookup(Context &ctx, Inode parent, const std::string &path, EntryParam &param,
		std::error_code &ec) {
	int ret = lizardfs_lookup_(ctx, parent, path.c_str(), param);
	ec = make_error_code(ret);
}

void Client::mknod(Context &ctx, Inode parent, const std::string &path, mode_t mode,
		dev_t rdev, EntryParam &param) {
	std::error_code ec;
	mknod(ctx, parent, path, mode, rdev, param, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::mknod(Context &ctx, Inode parent, const std::string &path, mode_t mode,
		dev_t rdev, EntryParam &param, std::error_code &ec) {
	int ret = lizardfs_mknod_(ctx, parent, path.c_str(), mode, rdev, param);
	ec = make_error_code(ret);
}

void Client::link(Context &ctx, Inode inode, Inode parent,
		const std::string &name, EntryParam &param) {
	std::error_code ec;
	link(ctx, inode, parent, name, param, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::link(Context &ctx, Inode inode, Inode parent,
		const std::string &name, EntryParam &param, std::error_code &ec) {
	int ret = lizardfs_link_(ctx, inode, parent, name.c_str(), param);
	ec = make_error_code(ret);
}

void Client::symlink(Context &ctx, const std::string &link, Inode parent,
		const std::string &name, EntryParam &param) {
	std::error_code ec;
	symlink(ctx, link, parent, name, param, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::symlink(Context &ctx, const std::string &link, Inode parent,
		const std::string &name, EntryParam &param, std::error_code &ec) {
	int ret = lizardfs_symlink_(ctx, link.c_str(), parent, name.c_str(), param);
	ec = make_error_code(ret);
}

Client::ReadDirReply Client::readdir(Context &ctx, FileInfo* fileinfo, off_t offset,
		size_t max_entries) {
	std::error_code ec;
	auto dir_entries = readdir(ctx, fileinfo, offset, max_entries, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return dir_entries;
}

Client::ReadDirReply Client::readdir(Context &ctx, FileInfo* fileinfo, off_t offset,
		size_t max_entries, std::error_code &ec) {
	auto ret = lizardfs_readdir_(ctx, fileinfo->opendirSessionID, fileinfo->inode, offset, max_entries);
	ec = make_error_code(ret.first);
	return ret.second;
}

std::string Client::readlink(Context &ctx, Inode inode) {
	std::error_code ec;
	std::string link = readlink(ctx, inode, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return link;
}

std::string Client::readlink(Context &ctx, Inode inode, std::error_code &ec) {
	std::string link;
	int ret = lizardfs_readlink_(ctx, inode, link);
	ec = make_error_code(ret);
	return link;
}

Client::ReadReservedReply Client::readreserved(Context &ctx, NamedInodeOffset offset,
	                                       NamedInodeOffset max_entries) {
	std::error_code ec;
	auto reserved_entries = readreserved(ctx, offset, max_entries, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return reserved_entries;
}

Client::ReadReservedReply Client::readreserved(Context &ctx, NamedInodeOffset offset,
	                                       NamedInodeOffset max_entries, std::error_code &ec) {
	auto ret = lizardfs_readreserved_(ctx, offset, max_entries);
	ec = make_error_code(ret.first);
	return ret.second;
}

Client::ReadTrashReply Client::readtrash(Context &ctx, NamedInodeOffset offset,
	                                 NamedInodeOffset max_entries) {
	std::error_code ec;
	auto trash_entries = readtrash(ctx, offset, max_entries, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return trash_entries;
}

Client::ReadTrashReply Client::readtrash(Context &ctx, NamedInodeOffset offset,
	                                 NamedInodeOffset max_entries, std::error_code &ec) {
	auto ret = lizardfs_readtrash_(ctx, offset, max_entries);
	ec = make_error_code(ret.first);
	return ret.second;
}

Client::FileInfo *Client::opendir(Context &ctx, Inode inode) {
	std::error_code ec;
	auto fileinfo = opendir(ctx, inode, ec);
	if (ec) {
		assert(!fileinfo);
		throw std::system_error(ec);
	}
	return fileinfo;
}

Client::FileInfo *Client::opendir(Context &ctx, Inode inode, std::error_code &ec) {
	int ret = lizardfs_opendir_(ctx, inode);
	ec = make_error_code(ret);
	if (ec) {
		return nullptr;
	}
	FileInfo *fileinfo = new FileInfo(inode, nextOpendirSessionID_++);
	std::lock_guard<std::mutex> guard(mutex_);
	fileinfos_.push_front(*fileinfo);
	return fileinfo;
}

void Client::releasedir(FileInfo* fileinfo) {
	std::error_code ec;
	releasedir(fileinfo, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::releasedir(FileInfo* fileinfo, std::error_code &ec) {
	assert(fileinfo != nullptr);
	int ret = lizardfs_releasedir_(fileinfo->inode, fileinfo->opendirSessionID);
	ec = make_error_code(ret);
	{
		std::lock_guard<std::mutex> guard(mutex_);
		fileinfos_.erase(fileinfos_.iterator_to(*fileinfo));
	}
	delete fileinfo;
}

void Client::rmdir(Context &ctx, Inode parent, const std::string &path) {
	std::error_code ec;
	rmdir(ctx, parent, path, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::rmdir(Context &ctx, Inode parent, const std::string &path, std::error_code &ec) {
	int ret = lizardfs_rmdir_(ctx, parent, path.c_str());
	ec = make_error_code(ret);
}

void Client::mkdir(Context &ctx, Inode parent, const std::string &path, mode_t mode,
	          Client::EntryParam &entry_param) {
	std::error_code ec;
	mkdir(ctx, parent, path, mode, entry_param, ec);
}

void Client::mkdir(Context &ctx, Inode parent, const std::string &path, mode_t mode,
	          Client::EntryParam &entry_param, std::error_code &ec) {
	int ret = lizardfs_mkdir_(ctx, parent, path.c_str(), mode, entry_param);
	ec = make_error_code(ret);
}

void Client::unlink(Context &ctx, Inode parent, const std::string &path) {
	std::error_code ec;
	unlink(ctx, parent, path, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::unlink(Context &ctx, Inode parent, const std::string &path, std::error_code &ec) {
	int ret = lizardfs_unlink_(ctx, parent, path.c_str());
	ec = make_error_code(ret);
}

void Client::undel(Context &ctx, Inode ino) {
	std::error_code ec;
	undel(ctx, ino, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::undel(Context &ctx, Inode ino, std::error_code &ec) {
	int ret = lizardfs_undel_(ctx, ino);
	ec = make_error_code(ret);
}

void Client::rename(Context &ctx, Inode parent, const std::string &path, Inode newparent,
	            const std::string &new_path) {
	std::error_code ec;
	rename(ctx, parent, path, newparent, new_path, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::rename(Context &ctx, Inode parent, const std::string &path, Inode newparent,
	            const std::string &new_path, std::error_code &ec) {
	int ret = lizardfs_rename_(ctx, parent, path.c_str(), newparent, new_path.c_str());
	ec = make_error_code(ret);
}

Client::FileInfo *Client::open(Context &ctx, Inode inode, int flags) {
	std::error_code ec;
	auto fileinfo = open(ctx, inode, flags, ec);
	if (ec) {
		assert(!fileinfo);
		throw std::system_error(ec);
	}
	return fileinfo;
}

Client::FileInfo *Client::open(Context &ctx, Inode inode, int flags, std::error_code &ec) {
	FileInfo *fileinfo = new FileInfo(inode);
	fileinfo->flags = flags;

	int ret = lizardfs_open_(ctx, inode, fileinfo);
	ec = make_error_code(ret);
	if (ec) {
		delete fileinfo;
		return nullptr;
	}
	std::lock_guard<std::mutex> guard(mutex_);
	fileinfos_.push_front(*fileinfo);
	return fileinfo;
}

void Client::getattr(Context &ctx, Inode inode, AttrReply &attr_reply) {
	std::error_code ec;
	getattr(ctx, inode, attr_reply, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::getattr(Context &ctx, Inode inode, AttrReply &attr_reply,
		std::error_code &ec) {
	int ret = lizardfs_getattr_(ctx, inode, attr_reply);
	ec = make_error_code(ret);
}

void Client::setattr(Context &ctx, Inode ino, struct stat *stbuf, int to_set,
	             AttrReply &attr_reply) {
	std::error_code ec;
	setattr(ctx, ino, stbuf, to_set, attr_reply, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::setattr(Context &ctx, Inode ino, struct stat *stbuf, int to_set,
	             AttrReply &attr_reply, std::error_code &ec) {
	int ret = lizardfs_setattr_(ctx, ino, stbuf, to_set, attr_reply);
	ec = make_error_code(ret);
}

Client::ReadResult Client::read(Context &ctx, FileInfo *fileinfo,
	                       off_t offset, std::size_t size) {
	std::error_code ec;
	auto ret = read(ctx, fileinfo, offset, size, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return ret;
}

Client::ReadResult Client::read(Context &ctx, FileInfo *fileinfo,
	                       off_t offset, std::size_t size, std::error_code &ec) {
	if (lizardfs_isSpecialInode_(fileinfo->inode)) {
		auto ret = lizardfs_read_special_inode_(ctx, fileinfo->inode, size, offset, fileinfo);
		ec = make_error_code(ret.first);
		if (ec) {
			return ReadResult();
		}
		return ReadResult(std::move(ret.second));
	} else {
		auto ret = lizardfs_read_(ctx, fileinfo->inode, size, offset, fileinfo);
		ec = make_error_code(ret.first);
		if (ec) {
			return ReadResult();
		}
		return std::move(ret.second);
	}
}

std::size_t Client::write(Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size,
		const char *buffer) {
	std::error_code ec;
	auto write_size = write(ctx, fileinfo, offset, size, buffer, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return write_size;
}

std::size_t Client::write(Context &ctx, FileInfo *fileinfo, off_t offset, std::size_t size,
		const char *buffer, std::error_code &ec) {
	std::pair<int, ssize_t> ret =
	        lizardfs_write_(ctx, fileinfo->inode, buffer, size, offset, fileinfo);
	ec = make_error_code(ret.first);
	return ec ? (std::size_t)0 : (std::size_t)ret.second;
}

void Client::release(FileInfo *fileinfo) {
	std::error_code ec;
	release(fileinfo, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::release(FileInfo *fileinfo, std::error_code &ec) {
	int ret = lizardfs_release_(fileinfo->inode, fileinfo);
	std::lock_guard<std::mutex> guard(mutex_);
	fileinfos_.erase(fileinfos_.iterator_to(*fileinfo));
	delete fileinfo;
	ec = make_error_code(ret);
}

void Client::flush(Context &ctx, FileInfo *fileinfo) {
	std::error_code ec;
	flush(ctx, fileinfo, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::flush(Context &ctx, FileInfo *fileinfo, std::error_code &ec) {
	int ret = lizardfs_flush_(ctx, fileinfo->inode, fileinfo);
	ec = make_error_code(ret);
}

void Client::fsync(Context &ctx, FileInfo *fileinfo) {
	std::error_code ec;
	fsync(ctx, fileinfo, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::fsync(Context &ctx, FileInfo *fileinfo, std::error_code &ec) {
	int ret = lizardfs_fsync_(ctx, fileinfo->inode, 0, fileinfo);
	ec = make_error_code(ret);
}

LizardClient::JobId Client::makesnapshot(Context &ctx, Inode src_inode, Inode dst_inode,
	                                 const std::string &dst_name, bool can_overwrite) {
	std::error_code ec;
	JobId job_id = makesnapshot(ctx, src_inode, dst_inode, dst_name, can_overwrite, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return job_id;
}

LizardClient::JobId Client::makesnapshot(Context &ctx, Inode src_inode, Inode dst_inode,
	                                 const std::string &dst_name, bool can_overwrite,
	                                 std::error_code &ec) {
	auto ret = lizardfs_makesnapshot_(ctx, src_inode, dst_inode, dst_name, can_overwrite);
	ec = make_error_code(ret.first);
	return ret.second;
}

std::string Client::getgoal(Context &ctx, Inode inode) {
	std::error_code ec;
	std::string res = getgoal(ctx, inode, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return res;
}

std::string Client::getgoal(Context &ctx, Inode inode, std::error_code &ec) {
	std::string goal;
	int ret = lizardfs_getgoal_(ctx, inode, goal);
	ec = make_error_code(ret);
	return goal;
}

void Client::setgoal(Context &ctx, Inode inode, const std::string &goal_name, uint8_t smode) {
	std::error_code ec;
	setgoal(ctx, inode, goal_name, smode, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::setgoal(Context &ctx, Inode inode, const std::string &goal_name,
	             uint8_t smode, std::error_code &ec) {
	int ret = lizardfs_setgoal_(ctx, inode, goal_name, smode);
	ec = make_error_code(ret);
}

void Client::statfs(Stats &stats) {
	std::error_code ec;
	statfs(stats, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::statfs(Stats &stats, std::error_code &ec) {
	int ret = lizardfs_statfs_(&stats.total_space, &stats.avail_space, &stats.trash_space,
	                 &stats.reserved_space, &stats.inodes);
	ec = make_error_code(ret);
}

void Client::setxattr(Context &ctx, Inode ino, const std::string &name,
	             const XattrBuffer &value, int flags) {
	std::error_code ec;
	setxattr(ctx, ino, name, value, flags, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::setxattr(Context &ctx, Inode ino, const std::string &name,
	              const XattrBuffer &value, int flags, std::error_code &ec) {
	int ret = lizardfs_setxattr_(ctx, ino, name.c_str(),
	                             (const char *)value.data(), value.size(), flags);
	ec = make_error_code(ret);
}

Client::XattrBuffer Client::getxattr(Context &ctx, Inode ino, const std::string &name) {
	std::error_code ec;
	auto ret = getxattr(ctx, ino, name, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return ret;
}

Client::XattrBuffer Client::getxattr(Context &ctx, Inode ino, const std::string &name,
	                                  std::error_code &ec) {
	LizardClient::XattrReply reply;
	int ret = lizardfs_getxattr_(ctx, ino, name.c_str(), kMaxXattrRequestSize, reply);
	ec = make_error_code(ret);
	return reply.valueBuffer;
}

Client::XattrBuffer Client::listxattr(Context &ctx, Inode ino) {
	std::error_code ec;
	auto ret = listxattr(ctx, ino, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return ret;
}

Client::XattrBuffer Client::listxattr(Context &ctx, Inode ino, std::error_code &ec) {
	LizardClient::XattrReply reply;
	int ret = lizardfs_listxattr_(ctx, ino, kMaxXattrRequestSize, reply);
	ec = make_error_code(ret);
	return reply.valueBuffer;
}

void Client::removexattr(Context &ctx, Inode ino, const std::string &name) {
	std::error_code ec;
	removexattr(ctx, ino, name, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::removexattr(Context &ctx, Inode ino, const std::string &name, std::error_code &ec) {
	int ret = lizardfs_removexattr_(ctx, ino, name.c_str());
	ec = make_error_code(ret);
}

void Client::setacl(Context &ctx, Inode ino, const RichACL &acl) {
	std::error_code ec;
	setacl(ctx, ino, std::move(acl), ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::setacl(Context &ctx, Inode ino, const RichACL &acl, std::error_code &ec) {
	try {
		std::vector<uint8_t> xattr = richAclConverter::objectToRichACLXattr(acl);
		setxattr(ctx, ino, kRichAclXattrName, xattr, 0, ec);
	} catch (...) {
		ec = make_error_code(LIZARDFS_ERROR_ENOATTR);
	}
}

RichACL Client::getacl(Context &ctx, Inode ino) {
	std::error_code ec;
	auto ret = getacl(ctx, ino, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return ret;
}

RichACL Client::getacl(Context &ctx, Inode ino, std::error_code &ec) {
	try {
		auto buffer = getxattr(ctx, ino, kRichAclXattrName, ec);
		if (ec) {
			return RichACL();
		}
		return richAclConverter::extractObjectFromRichACL(buffer.data(), buffer.size());
	} catch (...) {
		ec = make_error_code(LIZARDFS_ERROR_ENOATTR);
	}
	return RichACL();
}

std::vector<std::string> Client::toXattrList(const XattrBuffer &buffer) {
	std::vector<std::string> xattr_list;
	size_t base = 0;
	size_t length = 0;
	while (base < buffer.size()) {
		while (base + length < buffer.size() && buffer[base + length] != '\0') {
			length++;
		}
		if (base + length == buffer.size()) {
			break;
		}
		xattr_list.push_back(std::string((const char *)buffer.data() + base, length));
		base += length + 1;
		length = 0;
	}
	return xattr_list;
}

std::vector<ChunkWithAddressAndLabel> Client::getchunksinfo(Context &ctx, Inode ino,
	                                          uint32_t chunk_index, uint32_t chunk_count) {
	std::error_code ec;
	auto ret = getchunksinfo(ctx, ino, chunk_index, chunk_count, ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return ret;
}

std::vector<ChunkWithAddressAndLabel> Client::getchunksinfo(Context &ctx, Inode ino,
	                             uint32_t chunk_index, uint32_t chunk_count, std::error_code &ec) {
	auto ret = lizardfs_getchunksinfo_(ctx, ino, chunk_index, chunk_count);
	ec = make_error_code(ret.first);
	return ret.second;
}

std::vector<ChunkserverListEntry> Client::getchunkservers() {
	std::error_code ec;
	auto ret = getchunkservers(ec);
	if (ec) {
		throw std::system_error(ec);
	}
	return ret;
}

std::vector<ChunkserverListEntry> Client::getchunkservers(std::error_code &ec) {
	auto ret = lizardfs_getchunkservers_();
	ec = make_error_code(ret.first);
	return ret.second;
}

void Client::getlk(Context &ctx, Inode ino, FileInfo *fileinfo, FlockWrapper &lock) {
	std::error_code ec;
	getlk(ctx, ino, fileinfo, lock, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::getlk(Context &ctx, Inode ino, FileInfo *fileinfo, FlockWrapper &lock,
		std::error_code &ec) {
	int ret = lizardfs_getlk_(ctx, ino, fileinfo, lock);
	ec = make_error_code(ret);
}

void Client::setlk(Context &ctx, Inode ino, FileInfo *fileinfo, FlockWrapper &lock,
	           std::function<int(const lzfs_locks::InterruptData &)> handler) {
	std::error_code ec;
	setlk(ctx, ino, fileinfo, lock, handler, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::setlk(Context &ctx, Inode ino, FileInfo *fileinfo, FlockWrapper &lock,
	                    std::function<int(const lzfs_locks::InterruptData &)> handler,
	                    std::error_code &ec) {
	auto ret = lizardfs_setlk_send_(ctx, ino, fileinfo, lock);
	ec = make_error_code(ret.first);
	if (ec) {
		return;
	}
	lzfs_locks::InterruptData interrupt_data(fileinfo->lock_owner, ino, ret.second);
	if (handler) {
		int err = handler(interrupt_data);
		if (err != LIZARDFS_STATUS_OK) {
			ec = make_error_code(err);
			return;
		}
	}
	int err = lizardfs_setlk_recv_();
	ec = make_error_code(err);
}

void Client::setlk_interrupt(const lzfs_locks::InterruptData &data) {
	std::error_code ec;
	setlk_interrupt(data, ec);
	if (ec) {
		throw std::system_error(ec);
	}
}

void Client::setlk_interrupt(const lzfs_locks::InterruptData &data, std::error_code &ec) {
	int ret = lizardfs_setlk_interrupt_(data);
	ec = make_error_code(ret);
}
