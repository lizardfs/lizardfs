/*
   Copyright 2005-2010 Jakub Kruszona-Zawadzki, Gemius SA, 2013-2014 EditShare,
   2013-2015 Skytechnology sp. z o.o..

   This file was part of MooseFS and is part of LizardFS.

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
#include "master/filesystem_store.h"

#include <cstdio>
#include <vector>

#include "common/cwrap.h"
#include "common/main.h"
#include "common/setup.h"
#include "common/lizardfs_version.h"
#include "common/metadata.h"
#include "common/rotate_files.h"
#include "common/setup.h"

#include "master/changelog.h"
#include "master/filesystem.h"
#include "master/filesystem_xattr.h"
#include "master/filesystem_metadata.h"
#include "master/filesystem_node.h"
#include "master/filesystem_freenode.h"
#include "master/filesystem_operations.h"
#include "master/filesystem_checksum.h"
#include "master/locks.h"
#include "master/matoclserv.h"
#include "master/matomlserv.h"
#include "master/metadata_dumper.h"
#include "master/restore.h"

static void fs_store_marker(FILE *fd) {
	static std::vector<uint8_t> buffer;
	buffer.clear();
	uint32_t marker = 0;
	serialize(buffer, marker);
	if (fwrite(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
		syslog(LOG_NOTICE, "fwrite error");
		return;
	}
}


static void fs_store_acl(uint32_t id, const AccessControlList &acl, FILE *fd) {
	static std::vector<uint8_t> buffer;
	buffer.clear();
	uint32_t size = serializedSize(id, acl);
	serialize(buffer, size, id, acl);
	if (fwrite(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
		syslog(LOG_NOTICE, "fwrite error");
		return;
	}
}

void fs_store_acls(FILE *fd) {
	for (uint32_t i = 0; i < NODEHASHSIZE; ++i) {
		for (FSNode *p = gMetadata->nodehash[i]; p; p = p->next) {
			if (p->extendedAcl) {
				fs_store_acl(p->id, *p->extendedAcl, fd);
			}
		}
	}
	fs_store_marker(fd);

	for (uint32_t i = 0; i < NODEHASHSIZE; ++i) {
		for (FSNode *p = gMetadata->nodehash[i]; p; p = p->next) {
			FSNodeDirectory *dir_node = static_cast<FSNodeDirectory*>(p);
			if (p->type == FSNode::kDirectory && dir_node->defaultAcl) {
				fs_store_acl(dir_node->id, *dir_node->defaultAcl, fd);
			}
		}
	}
	fs_store_marker(fd);
}

static int fs_load_legacy_acl(FILE *fd, int ignoreflag) {
	static std::vector<uint8_t> buffer;
	buffer.clear();

	// initialize
	if (fd == nullptr) {
		return 0;
	}

	try {
		// Read size of the entry
		uint32_t size = 0;
		buffer.resize(serializedSize(size));
		if (fread(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
			throw Exception(std::string("read error: ") + strerr(errno), LIZARDFS_ERROR_IO);
		}
		deserialize(buffer, size);
		if (size == 0) {
			// this is end marker
			return 1;
		} else if (size > 10000000) {
			throw Exception("strange size of entry: " + std::to_string(size),
				LIZARDFS_ERROR_ERANGE);
		}

		// Read the entry
		buffer.resize(size);
		if (fread(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
			throw Exception(std::string("read error: ") + strerr(errno), LIZARDFS_ERROR_IO);
		}

		// Deserialize inode
		uint32_t inode;
		deserialize(buffer, inode);
		FSNode *p = fsnodes_id_to_node(inode);
		if (!p) {
			throw Exception("unknown inode: " + std::to_string(inode));
		}

		// Deserialize ACL
		std::unique_ptr<legacy::ExtendedAcl> extended_acl;
		std::unique_ptr<legacy::AccessControlList> default_acl;

		deserialize(buffer, inode, extended_acl, default_acl);

		if (extended_acl) {
			p->extendedAcl.reset(new AccessControlList((AccessControlList)*extended_acl));
		}
		if (p->type == FSNode::kDirectory && default_acl) {
			static_cast<FSNodeDirectory*>(p)->defaultAcl.reset(new AccessControlList((AccessControlList)*default_acl));
		}
		return 0;
	} catch (Exception &ex) {
		lzfs_pretty_syslog(LOG_ERR, "loading acl: %s", ex.what());
		if (!ignoreflag || ex.status() != LIZARDFS_STATUS_OK) {
			return -1;
		} else {
			return 0;
		}
	}
}

int fs_load_legacy_acls(FILE *fd, int ignoreflag) {
	fs_load_legacy_acl(NULL, ignoreflag);  // init
	int s = 0;
	do {
		s = fs_load_legacy_acl(fd, ignoreflag);
		if (s < 0) {
			return -1;
		}
	} while (s == 0);
	return 0;
}

static int fs_load_acl(FILE *fd, int ignoreflag, bool default_acl) {
	static std::vector<uint8_t> buffer;
	buffer.clear();

	try {
		// Read size of the entry
		uint32_t size = 0;
		buffer.resize(serializedSize(size));
		if (fread(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
			throw Exception(std::string("read error: ") + strerr(errno), LIZARDFS_ERROR_IO);
		}
		deserialize(buffer, size);
		if (size == 0) {
			// this is end marker
			return 1;
		} else if (size > 10000000) {
			throw Exception("strange size of entry: " + std::to_string(size),
				LIZARDFS_ERROR_ERANGE);
		}

		// Read the entry
		buffer.resize(size);
		if (fread(buffer.data(), 1, buffer.size(), fd) != buffer.size()) {
			throw Exception(std::string("read error: ") + strerr(errno), LIZARDFS_ERROR_IO);
		}

		// Deserialize inode
		uint32_t inode;
		deserialize(buffer, inode);
		FSNode *p = fsnodes_id_to_node(inode);
		if (!p) {
			throw Exception("unknown inode: " + std::to_string(inode));
		}

		// Deserialize ACL
		AccessControlList acl;

		deserialize(buffer, inode, acl);

		if (default_acl) {
			if (p->type == FSNode::kDirectory) {
				static_cast<FSNodeDirectory*>(p)->defaultAcl.reset(new AccessControlList(std::move(acl)));
			}
		} else {
			p->extendedAcl.reset(new AccessControlList(std::move(acl)));
		}

		return 0;
	} catch (Exception &ex) {
		lzfs_pretty_syslog(LOG_ERR, "loading acl: %s", ex.what());
		if (!ignoreflag || ex.status() != LIZARDFS_STATUS_OK) {
			return -1;
		} else {
			return 0;
		}
	}
}

int fs_load_acls(FILE *fd, int ignoreflag) {
	int s;

	do {
		s = fs_load_acl(fd, ignoreflag, false);
		if (s < 0) {
			return -1;
		}
	} while (s == 0);

	do {
		s = fs_load_acl(fd, ignoreflag, true);
		if (s < 0) {
			return -1;
		}
	} while (s == 0);

	return 0;
}
