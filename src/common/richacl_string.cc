/*
   Copyright 2017 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.

   -- based on richacl_to_text.c, which is:
   Copyright (C) 2006, 2009, 2010  Novell, Inc.
   Copyright (C) 2015  Red Hat, Inc.
   Written by Andreas Gruenbacher <agruenba@redhat.com>
 */

#include "common/platform.h"

#include <array>
#include <cassert>
#include <string>
#include "common/richacl.h"


struct richacl_type_value {
	unsigned short  e_type;
	const char      *e_name;
};

struct richacl_flag_bit {
	char            a_char;
	unsigned char   a_flag;
	const char      *a_name;
};

struct richace_flag_bit {
	unsigned short  e_flag;
	char            e_char;
	const char      *e_name;
};

struct richacl_mask_flag {
	unsigned int    e_mask;
	char            e_char;
	const char      *e_name;
	int             e_context;
};

static const std::array<richacl_flag_bit, 5> kAclFlagBits = {{
	{ 'm', RichACL::kMasked, "masked" },
	{ 'w', RichACL::kWriteThrough, "write_through" },
	{ 'a', RichACL::kAutoInherit, "auto_inherit" },
	{ 'p', RichACL::kProtected, "protected" },
	{ 'd', RichACL::kDefaulted, "defaulted" },
}};

static const std::array<richacl_type_value, 2> kTypeValues = {{
	{ RichACL::Ace::kAccessAllowedAceType, "A" },
	{ RichACL::Ace::kAccessDeniedAceType,  "D" },
}};

const std::array<richace_flag_bit, 6> kAceFlagBits = {{
	{RichACL::Ace::kFileInheritAce, 'f', "file_inherit"},
	{RichACL::Ace::kDirectoryInheritAce, 'd', "dir_inherit"},
	{RichACL::Ace::kNoPropagateInheritAce, 'n', "no_propagate"},
	{RichACL::Ace::kInheritOnlyAce, 'i', "inherit_only"},
	{RichACL::Ace::kInheritedAce, 'a', "inherited"},
	{RichACL::Ace::kSpecialWho, 'S', "special_who"},
}};

#define MASK_BIT(c, name, str) \
	{ RichACL::Ace:: name, c, str, RICHACL_TEXT_FILE_CONTEXT | \
				 RICHACL_TEXT_DIRECTORY_CONTEXT }
#define FILE_MASK_BIT(c, name, str) \
	{ RichACL::Ace:: name, c, str, RICHACL_TEXT_FILE_CONTEXT }
#define DIRECTORY_MASK_BIT(c, name, str) \
	{ RichACL::Ace:: name, c, str, RICHACL_TEXT_DIRECTORY_CONTEXT }

#define RICHACL_TEXT_FILE_CONTEXT       2
#define RICHACL_TEXT_DIRECTORY_CONTEXT  4

const std::array<richacl_mask_flag, 19> kMaskFlags = {{
	FILE_MASK_BIT('r', kReadData, "read_data"),
	DIRECTORY_MASK_BIT('r', kListDirectory, "list_directory"),
	FILE_MASK_BIT('w', kWriteData, "write_data"),
	DIRECTORY_MASK_BIT('w', kAddFile, "add_file"),
	FILE_MASK_BIT('p', kAppendData, "append_data"),
	DIRECTORY_MASK_BIT('p', kAddSubdirectory, "add_subdirectory"),
	MASK_BIT('x', kExecute, "execute"),
	/* DELETE_CHILD is only meaningful for directories but it might also
	   be set in an ACE of a file, so print it in file context as well.  */
	MASK_BIT('d', kDeleteChild, "delete_child"),
	MASK_BIT('D', kDelete, "delete"),
	MASK_BIT('a', kReadAttributes, "read_attributes"),
	MASK_BIT('A', kWriteAttributes, "write_attributes"),
	MASK_BIT('R', kReadNamedAttrs, "read_named_attrs"),
	MASK_BIT('W', kWriteNamedAttrs, "write_named_attrs"),
	MASK_BIT('c', kReadAcl, "read_acl"),
	MASK_BIT('C', kWriteAcl, "write_acl"),
	MASK_BIT('o', kWriteOwner, "write_owner"),
	MASK_BIT('S', kSynchronize, "synchronize"),
	MASK_BIT('e', kWriteRetention, "write_retention"),
	MASK_BIT('E', kWriteRetentionHold, "write_retention_hold"),
}};

#undef MASK_BIT
#undef FILE_MASK_BIT
#undef DIRECTORY_MASK_BIT

static std::string writeACLFlags(unsigned char flags) {
	std::string str;
	if (!flags) {
		return str;
	}

	for (const richacl_flag_bit &flag_bit : kAclFlagBits) {
		if (flags & flag_bit.a_flag) {
			flags &= ~flag_bit.a_flag;
			str += flag_bit.a_char;
		}
	}
	return str;
}

static std::string writeType(unsigned short type) {
	std::string str;
	size_t i;

	for (i = 0; i < kTypeValues.size(); ++i) {
		if (type == kTypeValues[i].e_type) {
			str += kTypeValues[i].e_name;
			break;
		}
	}
	if (i == kTypeValues.size()) {
		str += type;
	}
	return str;
}

static std::string writeAceFlags(unsigned short flags) {
	std::string str;
	flags &= ~(RichACL::Ace::kIdentifierGroup | RichACL::Ace::kSpecialWho);
	for (const richace_flag_bit &flag_bit : kAceFlagBits) {
		if (flags & flag_bit.e_flag) {
			flags &= ~flag_bit.e_flag;
			str += flag_bit.e_char;
		}

	}
	return str;
}

static std::string writeMask(unsigned int mask) {
	std::string str;
	for (const richacl_mask_flag &mask_flag : kMaskFlags) {
		if (mask & mask_flag.e_mask) {
			mask &= ~mask_flag.e_mask;
			str += mask_flag.e_char;
		}
	}
	return str;
}

static std::string writeIdentifier(const RichACL::Ace &ace) {
	std::string str;
	if (ace.flags & RichACL::Ace::kSpecialWho) {
		switch (ace.id) {
		case RichACL::Ace::kOwnerSpecialId:
		    str += 'O';
		    break;
		case RichACL::Ace::kGroupSpecialId:
		    str += 'G';
		    break;
		case RichACL::Ace::kEveryoneSpecialId:
		    str += 'E';
		    break;
		}
	} else if (ace.flags & RichACL::Ace::kIdentifierGroup) {
		str += 'g' + std::to_string(ace.id);
	} else {
		str += 'u' + std::to_string(ace.id);
	}
	return str;
}

std::string RichACL::Ace::toString() const {
	std::string str;
	str += writeMask(mask) + ':';
	str += writeAceFlags(flags) + ':';
	str += writeType(type) + ':';
	str += writeIdentifier(*this) + '/';
	return str;
}

std::string RichACL::toString() const {
	std::string str;

	str += writeACLFlags(flags_) + '|';
	str += writeMask(owner_mask_) + '|';
	str += writeMask(group_mask_) + '|';
	str += writeMask(other_mask_) + '|';
	for (const RichACL::Ace &ace : *this) {
		str += ace.toString();
	}
	return str;
}

uint16_t getAclFlags(const std::string &str, size_t start, size_t delim) {
	uint16_t flags = 0;
	if (start == delim) {
		return flags;
	}
	for (const richacl_flag_bit &flag_bit : kAclFlagBits) {
		if (str[start] == flag_bit.a_char) {
			flags |= flag_bit.a_flag;
			start++;
			if (start == delim) {
				break;
			}
		}
	}
	// Overkill check for not sorted flag string
	for (; start != delim; ++start) {
		auto it = std::find_if(kAclFlagBits.begin(), kAclFlagBits.end(), [&str, start](const richacl_flag_bit &flag_bit) {
			return str[start] == flag_bit.a_char;
		});
		if (it == kAclFlagBits.end()) {
			throw RichACL::FormatException("Unsupported ace flag " + str.substr(start, 1));
		}
		flags |= it->a_flag;
	}
	return flags;
}

void setAceIdentifier(RichACL::Ace &ace, const std::string &str, size_t start, size_t delim) {
	switch (str[start]) {
	case 'O':
		ace.id = RichACL::Ace::kOwnerSpecialId;
		ace.flags |= RichACL::Ace::kSpecialWho;
		break;
	case 'G':
		ace.id = RichACL::Ace::kGroupSpecialId;
		ace.flags |= RichACL::Ace::kSpecialWho;
		break;
	case 'E':
		ace.id = RichACL::Ace::kEveryoneSpecialId;
		ace.flags |= RichACL::Ace::kSpecialWho;
		break;
	case 'u':
		start++;
		try {
			ace.id = std::stoull(str.substr(start, delim), nullptr, 10);
		} catch (...) {
			ace.id = std::numeric_limits<uint32_t>::max();
		}
		break;
	case 'g':
		start++;
		try {
			ace.id = std::stoull(str.substr(start, delim), nullptr, 10);
		} catch (...) {
			ace.id = std::numeric_limits<uint32_t>::max();
		}
		ace.flags |= RichACL::Ace::kIdentifierGroup;
		break;
	default:
		throw RichACL::FormatException("unsupported ace identifier " + str.substr(start, 1));
	}
}

uint32_t getAceMask(const std::string &str, size_t start, size_t delim) {
	uint32_t mask = 0;
	if (start == delim) {
		return mask;
	}
	for (const richacl_mask_flag &mask_flag : kMaskFlags) {
		if (str[start] == mask_flag.e_char) {
			mask |= mask_flag.e_mask;
			start++;
			if (start == delim) {
				break;
			}
		}
	}
	// Overkill check for not sorted flag string
	for (; start != delim; ++start) {
		auto it = std::find_if(kMaskFlags.begin(), kMaskFlags.end(), [&str, start](const richacl_mask_flag &flag_bit) {
			return str[start] == flag_bit.e_char;
		});
		if (it == kMaskFlags.end()) {
			throw RichACL::FormatException("Unsupported ace mask " + str.substr(start, 1));
		}
		mask |= it->e_mask;
	}
	return mask;
}

uint32_t getAceFlags(const std::string &str, size_t start, size_t delim) {
	uint32_t flags = 0;
	if (start == delim) {
		return flags;
	}
	for (const richace_flag_bit &ace_bit : kAceFlagBits) {
		if (str[start] == ace_bit.e_char) {
			flags |= ace_bit.e_flag;
			start++;
			if (start == delim) {
				break;
			}
		}
	}
	// Overkill check for not sorted flag string
	for (; start != delim; ++start) {
		auto it = std::find_if(kAceFlagBits.begin(), kAceFlagBits.end(), [&str, start](const richace_flag_bit &flag_bit) {
			return str[start] == flag_bit.e_char;
		});
		if (it == kAceFlagBits.end()) {
			throw RichACL::FormatException("Unsupported ace flag " + str.substr(start, 1));
		}
		flags |= it->e_flag;
	}
	return flags;
}

uint32_t getAceType(const std::string &str, size_t start, size_t /*delim*/) {
	if (str[start] == 'A') {
		return RichACL::Ace::kAccessAllowedAceType;
	}
	if (str[start] == 'D') {
		return RichACL::Ace::kAccessDeniedAceType;
	}
	throw RichACL::FormatException("unsupported ace type " + str.substr(start, 1));
}

RichACL RichACL::fromString(const std::string &str) {
	RichACL acl;

	size_t start = 0;
	size_t delim = str.find_first_of('|');
	if (delim == std::string::npos) {
		throw FormatException("string too short, cannot parse acl flags");
	}
	acl.flags_ = getAclFlags(str, 0, delim);
	start = delim + 1;
	delim = str.find_first_of('|', start);
	if (delim == std::string::npos) {
		throw FormatException("string too short, cannot parse acl owner mask");
	}
	acl.owner_mask_ = getAceMask(str, start, delim);
	start = delim + 1;
	delim = str.find_first_of('|', start);
	if (delim == std::string::npos) {
		throw FormatException("string too short, cannot parse acl group mask");
	}
	acl.group_mask_ = getAceMask(str, start, delim);
	start = delim + 1;
	delim = str.find_first_of('|', start);
	if (delim == std::string::npos) {
		throw FormatException("string too short, cannot parse acl other mask");
	}
	acl.other_mask_ = getAceMask(str, start, delim);

	start = delim + 1;
	while (start < str.size()) {
		Ace ace;
		delim = str.find_first_of(':', start);
		if (delim == std::string::npos) {
			throw FormatException("string too short, cannot parse ace mask");
		}
		ace.mask = getAceMask(str, start, delim);
		start = delim + 1;
		delim = str.find_first_of(':', start);
		if (delim == std::string::npos) {
			throw FormatException("string too short, cannot parse ace flags");
		}
		ace.flags = getAceFlags(str, start, delim);
		start = delim + 1;
		delim = str.find_first_of(':', start);
		if (delim == std::string::npos) {
			throw FormatException("string too short, cannot parse ace type");
		}
		ace.type = getAceType(str, start, delim);
		start = delim + 1;
		delim = str.find_first_of('/', start);
		if (delim == std::string::npos) {
			throw FormatException("string too short, cannot parse ace id");
		}
		setAceIdentifier(ace, str, start, delim);
		start = delim + 1;
		acl.insert(ace);
	}

	return acl;
}
