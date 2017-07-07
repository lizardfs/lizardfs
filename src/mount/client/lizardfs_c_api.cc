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

#include <cassert>
#include <system_error>

#include "lizardfs_c_api.h"
#include "common/lizardfs_error_codes.h"
#include "common/small_vector.h"
#include "mount/client/iovec_traits.h"

#include "client.h"

using namespace lizardfs;

#ifdef LIZARDFS_HAVE_THREAD_LOCAL
static thread_local liz_err_t gLastErrorCode(LIZARDFS_STATUS_OK);
#else
static __thread liz_err_t gLastErrorCode(LIZARDFS_STATUS_OK);
#endif

static void to_entry(const Client::EntryParam &param, liz_entry *entry) {
	assert(entry);
	entry->ino = param.ino;
	entry->generation = param.generation;
	entry->attr = param.attr;
	entry->attr_timeout = param.attr_timeout;
	entry->entry_timeout = param.entry_timeout;
}

static void to_attr_reply(const Client::AttrReply &attr_reply, liz_attr_reply *reply) {
	assert(reply);
	reply->attr = attr_reply.attr;
	reply->attr_timeout = attr_reply.attrTimeout;
}

static void to_stat(const Client::Stats &stats, liz_stat_t *buf) {
	assert(buf);
	buf->total_space = stats.total_space;
	buf->avail_space = stats.avail_space;
	buf->trash_space = stats.trash_space;
	buf->reserved_space = stats.reserved_space;
	buf->inodes = stats.inodes;
}

liz_err_t liz_last_err() {
	return gLastErrorCode;
}

liz_err_t liz_error_conv(liz_err_t lizardfs_error_code) {
	if (lizardfs_error_code < 0) {
		return EINVAL;
	} else {
		return lizardfs_error_conv(lizardfs_error_code);
	}
}

const char *liz_error_string(liz_err_t lizardfs_error_code) {
	return lizardfs_error_string(lizardfs_error_code);
}

liz_context_t *liz_create_context() {
	try {
		Client::Context *ret = new Client::Context(getuid(), getgid(), getpid(), 0);
		return (liz_context_t *)ret;
	} catch (...) {
		return nullptr;
	}
}

liz_context_t *liz_create_user_context(uid_t uid, gid_t gid, pid_t pid, mode_t umask) {
	try {
		Client::Context *ret = new Client::Context(uid, gid, pid, umask);
		return (liz_context_t *)ret;
	} catch (...) {
		return nullptr;
	}
}

void liz_destroy_context(liz_context_t *ctx) {
	Client::Context *client_ctx = (Client::Context *)ctx;
	delete client_ctx;
}

liz_t *liz_init(const char *host, const char *port, const char *mountpoint) {
	try {
		Client *ret = new Client(host, port, mountpoint);
		gLastErrorCode = LIZARDFS_STATUS_OK;
		return (liz_t *)ret;
	} catch (...) {
		gLastErrorCode = LIZARDFS_ERROR_CANTCONNECT;
		return nullptr;
	}
}

int liz_update_groups(liz_t *instance, liz_context_t *ctx, gid_t *gids, int gid_num) {
	Client &client = *(Client *)instance;
	Client::Context &client_ctx = *(Client::Context *)ctx;
	Client::Context::GroupsContainer gids_backup(std::move(client_ctx.gids));
	try {
		client_ctx.gids.assign(gids, gids + gid_num);
		std::error_code ec;
		client.updateGroups(client_ctx, ec);
		gLastErrorCode = ec.value();
		if (ec) {
			client_ctx.gids = std::move(gids_backup);
			return -1;
		}
	} catch (...) {
		client_ctx.gids = std::move(gids_backup);
		gLastErrorCode = LIZARDFS_ERROR_GROUPNOTREGISTERED;
		return -1;
	}
	return 0;
}

int liz_lookup(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *path,
	       liz_entry *entry) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	Client::EntryParam entry_param;
	std::error_code ec;
	client.lookup(context, parent, path, entry_param, ec);
	gLastErrorCode = ec.value();
	if (ec) {
		return -1;
	} else {
		to_entry(entry_param, entry);
	}
	return 0;
}

int liz_mknod(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *path,
	      mode_t mode, liz_entry *entry) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	Client::EntryParam entry_param;
	std::error_code ec;
	client.mknod(context, parent, path, mode, entry_param, ec);
	gLastErrorCode = ec.value();
	if (ec) {
		return -1;
	} else {
		to_entry(entry_param, entry);
	}
	return 0;
}

liz_fileinfo *liz_open(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, int flags) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	liz_fileinfo *fi = (liz_fileinfo *)client.open(context, inode, flags, ec);
	gLastErrorCode = ec.value();
	return fi;
}

ssize_t liz_read(liz_t *instance, liz_context_t *ctx, liz_fileinfo *fileinfo, off_t offset,
	         size_t size, char *buffer) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	auto ret = client.read(context, (Client::FileInfo *)fileinfo, offset, size, ec);
	if (ec) {
		gLastErrorCode = ec.value();
		return -1;
	}
	return ret.copyToBuffer((uint8_t *)buffer, offset, size);
}

ssize_t liz_readv(liz_t *instance, liz_context_t *ctx, liz_fileinfo *fileinfo, off_t offset,
	          size_t size, const struct iovec *iov, int iovcnt) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	auto ret = client.read(context, (Client::FileInfo *)fileinfo, offset, size, ec);
	if (ec) {
		gLastErrorCode = ec.value();
		return -1;
	}
	small_vector<struct iovec, 8> reply;
	ret.toIoVec(reply, offset, size);
	return copyIoVec(iov, iovcnt, reply.data(), reply.size());
}

ssize_t liz_write(liz_t *instance, liz_context_t *ctx, liz_fileinfo *fileinfo, off_t offset,
	          size_t size, const char *buffer){
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	std::size_t write_ret = client.write(context, (Client::FileInfo *)fileinfo, offset, size,
	                                     buffer, ec);
	gLastErrorCode = ec.value();
	return ec ? -1 : write_ret;
}

int liz_release(liz_t *instance, liz_context_t *ctx, liz_fileinfo *fileinfo) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	client.release(context, (Client::FileInfo *)fileinfo, ec);
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_flush(liz_t *instance, liz_context_t *ctx, liz_fileinfo *fileinfo) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	client.flush(context, (Client::FileInfo *)fileinfo, ec);
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_getattr(liz_t *instance, liz_context_t *ctx, liz_inode_t inode,
	        liz_attr_reply *reply) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	Client::AttrReply r;
	std::error_code ec;
	client.getattr(context, inode, r, ec);
	gLastErrorCode = ec.value();
	if (ec) {
		return -1;
	} else if (reply) {
		to_attr_reply(r, reply);
	}
	return 0;
}

void liz_destroy(liz_t *instance) {
	Client *client = (Client *)instance;
	delete client;
}

struct liz_fileinfo *liz_opendir(liz_t *instance, liz_context_t *ctx, liz_inode_t inode) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	liz_fileinfo *fi = (liz_fileinfo *)client.opendir(context, inode, ec);
	gLastErrorCode = ec.value();
	return fi;
}

int liz_readdir(liz_t *instance, liz_context_t *ctx, struct liz_fileinfo *fileinfo,
		off_t offset, size_t max_entries, struct liz_direntry *buf, size_t *num_entries) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	Client::ReadDirReply reply = client.readdir(context, (Client::FileInfo *)fileinfo,
	                                            offset, max_entries, ec);
	*num_entries = 0;
	gLastErrorCode = ec.value();
	if (ec) {
		return -1;
	}
	if (reply.empty()) {
		return 0;
	}

	size_t total_name_size = 0;
	for (const auto &dir_entry : reply) {
		total_name_size += dir_entry.name.size() + 1;
	}

	char *p_namebuf;
	try {
		p_namebuf = new char[total_name_size];
	} catch (...) {
		gLastErrorCode = ENOMEM;
		return -1;
	}

	for (const auto &dir_entry : reply) {
		buf->name = p_namebuf;
		buf->attr = dir_entry.attr;
		buf->next_entry_offset = dir_entry.nextEntryOffset;
		buf++;

		auto s = dir_entry.name.size();
		dir_entry.name.copy(p_namebuf, s);
		p_namebuf[s] = '\0';
		p_namebuf += s + 1;
	}

	*num_entries = reply.size();
	return 0;
}

void liz_destroy_direntry(struct liz_direntry *buf, size_t num_entries) {
	assert(num_entries > 0);
	(void)num_entries;
	delete[] buf->name;
}

int liz_releasedir(liz_t *instance, liz_context_t *ctx, struct liz_fileinfo *fileinfo) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	client.releasedir(context, (Client::FileInfo *)fileinfo, ec);
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_mkdir(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *path,
		mode_t mode, struct liz_entry *entry) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	Client::EntryParam entry_param;
	std::error_code ec;
	client.mkdir(context, parent, path, mode, entry_param, ec);
	if (!ec) {
		to_entry(entry_param, entry);
	}
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_rmdir(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *path) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	client.rmdir(context, parent, path, ec);
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_makesnapshot(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, liz_inode_t dst_parent,
	             const char *dst_name, int can_overwrite, uint32_t *job_id) {
	static_assert(sizeof(LizardClient::JobId) <= sizeof(uint32_t), "JobId type too large");
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	LizardClient::JobId ret = client.makesnapshot(context, inode, dst_parent, dst_name, can_overwrite, ec);
	if (job_id) {
		*job_id = ret;
	}
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_getgoal(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, char *goal_name) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	std::string goal = client.getgoal(context, inode, ec);
	gLastErrorCode = ec.value();
	if (ec) {
		return -1;
	}
	size_t copied = goal.copy(goal_name, LIZARDFS_MAX_GOAL_NAME - 1);
	goal_name[copied] = '\0';

	return 0;
}

int liz_setgoal(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, const char *goal_name,
	        int is_recursive) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	client.setgoal(context, inode, goal_name, is_recursive ? SMODE_RMASK : 0, ec);
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_unlink(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *path) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	client.unlink(context, parent, path, ec);
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_undel(liz_t *instance, liz_context_t *ctx, liz_inode_t inode) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	client.undel(context, inode, ec);
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_setattr(liz_t *instance, liz_context_t *ctx, liz_inode_t inode, struct stat *stbuf, int to_set,
		struct liz_fileinfo *fileinfo, struct liz_attr_reply *reply) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	Client::AttrReply r;
	std::error_code ec;
	client.setattr(context, inode, stbuf, to_set, (Client::FileInfo *)fileinfo, r, ec);
	if (!ec) {
		to_attr_reply(r, reply);
	}
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_fsync(liz_t *instance, liz_context_t *ctx, struct liz_fileinfo *fileinfo) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	client.fsync(context, (Client::FileInfo *)fileinfo, ec);
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_rename(liz_t *instance, liz_context_t *ctx, liz_inode_t parent, const char *path,
	       liz_inode_t new_parent, const char *new_path) {
	Client &client = *(Client *)instance;
	Client::Context &context = *(Client::Context *)ctx;
	std::error_code ec;
	client.rename(context, parent, path, new_parent, new_path, ec);
	gLastErrorCode = ec.value();
	return ec ? -1 : 0;
}

int liz_statfs(liz_t *instance, liz_stat_t *buf) {
	Client &client = *(Client *)instance;
	Client::Stats stats;
	std::error_code ec;
	client.statfs(stats, ec);
	gLastErrorCode = ec.value();
	if (ec) {
		return -1;
	}
	to_stat(stats, buf);
	return 0;
}
