#pragma once

#include "common/platform.h"

#include <cstring>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/massert.h"

#ifdef WORDS_BIGENDIAN
# define IF_BIGENDIAN(F) F
#else
# define IF_BIGENDIAN(F)
#endif
#ifndef le32toh
# define le32toh(X) (IF_BIGENDIAN(ntohl)(X))
# define htole32(X) (IF_BIGENDIAN(htonl)(X))
# define le16toh(X) (IF_BIGENDIAN(ntohs)(X))
# define htole16(X) (IF_BIGENDIAN(htons)(X))
#endif

// These #defines and structs were extracted from kernel sources

/* Extended attribute names */
#define POSIX_ACL_XATTR_ACCESS  "system.posix_acl_access"
#define POSIX_ACL_XATTR_DEFAULT "system.posix_acl_default"

#define ACL_UNDEFINED_ID    (-1U)

/* e_tag entry in struct posix_acl_entry */
#define ACL_USER_OBJ        (0x01)
#define ACL_USER            (0x02)
#define ACL_GROUP_OBJ       (0x04)
#define ACL_GROUP           (0x08)
#define ACL_MASK            (0x10)
#define ACL_OTHER           (0x20)

/* permissions in the e_perm field */
#define ACL_READ            (0x04)
#define ACL_WRITE           (0x02)
#define ACL_EXECUTE         (0x01)

struct PosixAclXattrEntry {
	int16_t tag;
	uint16_t perm;
	uint32_t id;

	size_t write(uint8_t* destination) const {
		PosixAclXattrEntry* entry = reinterpret_cast<PosixAclXattrEntry*>(destination);
		entry->tag = htole16(tag);
		entry->perm = htole16(perm);
		entry->id = htole32(id);
		return sizeof(PosixAclXattrEntry);
	}

	size_t read(const uint8_t* source) {
		const PosixAclXattrEntry* entry = reinterpret_cast<const PosixAclXattrEntry*>(source);
		tag = le16toh(entry->tag);
		perm = le16toh(entry->perm);
		id = le32toh(entry->id);
		return sizeof(PosixAclXattrEntry);
	}

	bool operator==(const PosixAclXattrEntry& entry) const {
		return (tag == entry.tag) && (perm == entry.perm) && (id == entry.id);
	}
};

/* Supported ACL a_version fields */
#define POSIX_ACL_XATTR_VERSION 0x0002

struct PosixAclXattr {
	uint32_t version;
	std::vector<PosixAclXattrEntry> entries;

	size_t rawSize() const {
		return sizeof(version) + entries.size() * sizeof(PosixAclXattrEntry);
	}

	size_t write(uint8_t* xattrBuffer) const {
		uint32_t leVersion = htole32(version);
		memcpy(xattrBuffer, &leVersion, sizeof(version));
		size_t bytesCount = sizeof(version);
		for (const auto& entry : entries) {
			bytesCount += entry.write(xattrBuffer + bytesCount);
		}
		return bytesCount;
	}

	void read(const uint8_t* xattrBuffer, uint32_t bytesInBuffer) {
		sassert(entries.empty());
		sassert(bytesInBuffer % sizeof(PosixAclXattrEntry) == sizeof(version));

		uint32_t leVersion;
		memcpy(&leVersion, xattrBuffer, sizeof(version));
		version = le32toh(leVersion);
		size_t bytesCount = sizeof(version);
		while (bytesCount < bytesInBuffer) {
			entries.emplace_back();
			bytesCount += entries.back().read(xattrBuffer + bytesCount);
		}
		if (bytesCount != bytesInBuffer) {
			throw Exception("Too many bytes read from buffer");
		}
	}

	void reset() {
		version = 0;
		entries.clear();
	}

	bool operator==(const PosixAclXattr& xattr) const {
		return (version == xattr.version) && (entries == xattr.entries);
	}
};
