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
	entry->ino = param.ino;
	entry->generation = param.generation;
	entry->attr = param.attr;
	entry->attr_timeout = param.attr_timeout;
	entry->entry_timeout = param.entry_timeout;
}

static void to_attr_reply(const Client::AttrReply &attr_reply, liz_attr_reply *reply) {
	reply->attr = attr_reply.attr;
	reply->attr_timeout = attr_reply.attrTimeout;
}

liz_err_t liz_last_err() {
	return gLastErrorCode;
}

liz_err_t liz_error_conv(int lizardfs_error_code) {
	if (lizardfs_error_code < 0) {
		return EINVAL;
	} else {
		return lizardfs_error_conv(lizardfs_error_code);
	}
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
