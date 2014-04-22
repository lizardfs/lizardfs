#include "common/acl_converter.h"

#include "common/posix_acl_xattr.h"

static constexpr uint16_t kUserPermissionsModeOffset  = 0100;
static constexpr uint16_t kGroupPermissionsModeOffset = 010;
static constexpr uint16_t kOtherPermissionsModeOffset = 01;

static bool checkPermissions(uint16_t perm) {
	return (perm & ~(ACL_READ | ACL_WRITE | ACL_EXECUTE)) == 0;
}

static uint16_t assertPermissionsOnTheFly(uint16_t perm) {
	if (!checkPermissions(perm)) {
		throw aclConverter::ConversionErrorException("Invalid permissions mask");
	}
	return perm;
}

static uint16_t extractMask(uint16_t mode, uint16_t offset) {
	uint16_t mask = uint16_t((mode / offset) & 07);
	return assertPermissionsOnTheFly(mask);
}

static void insertToMode(uint16_t& mode, uint16_t offset, uint8_t newMask) {
	uint8_t oldMask = (mode / offset) & 07;
	mode -= oldMask * offset;
	mode += newMask * offset;
}

static void prepareExtendedAcl(AccessControlList& acl) {
	if (!acl.extendedAcl) {
		acl.extendedAcl.reset(new ExtendedAcl);
		acl.extendedAcl->setOwningGroupMask(extractMask(acl.mode, kGroupPermissionsModeOffset));
	}
}

static void validateNonIdEntry(uint8_t tags, const PosixAclXattrEntry& entry) {
	// These entries can appear only once...
	if (tags & entry.tag) {
		throw aclConverter::ConversionErrorException("Entry duplication");
	}
	// ...and with undefined id
	if (entry.id != ACL_UNDEFINED_ID) {
		throw aclConverter::ConversionErrorException("Entry with defined ID");
	}
}

static void validateIdEntry(const AccessControlList& acl, const PosixAclXattrEntry& entry) {
	if (acl.extendedAcl &&
			acl.extendedAcl->hasEntryFor(ExtendedAcl::EntryType::kNamedGroup, entry.id)) {
		throw IncorrectDeserializationException("Group entry duplication");
	}
}

AccessControlList aclConverter::xattrToAclObject(const uint8_t* buffer, uint32_t bufferSize) {
	// Load xattr ACL structure from raw data
	PosixAclXattr xattr;
	try {
		xattr.read(buffer, bufferSize);
	} catch (Exception&) {
		throw ConversionErrorException("Data doesn't contain ACL");
	}

	// Data validation and conversion
	if (xattr.version != POSIX_ACL_XATTR_VERSION) {
		throw ConversionErrorException("Incorrect POSIX ACL xattr version: " +
				std::to_string(xattr.version));
	}

	AccessControlList acl(0);
	// NOTE Documentation says nothing about the order of entries (it's supposed
	//      to be ascending by a tag value), so we allow any order.
	uint8_t appearedTagsBitmask = 0x00;
	for (const PosixAclXattrEntry& entry : xattr.entries) {
		assertPermissionsOnTheFly(entry.perm); // Check permissions mask
		switch (entry.tag) {
			case ACL_USER_OBJ:
				validateNonIdEntry(appearedTagsBitmask, entry);
				insertToMode(acl.mode, kUserPermissionsModeOffset, entry.perm);
				break;
			case ACL_USER:
				validateIdEntry(acl, entry);
				prepareExtendedAcl(acl);
				acl.extendedAcl->addNamedUser(entry.id, entry.perm);
				break;
			case ACL_GROUP_OBJ:
				validateNonIdEntry(appearedTagsBitmask, entry);
				// If eacl exists, store owning group permissions there
				if (acl.extendedAcl) {
					acl.extendedAcl->setOwningGroupMask(entry.perm);
				} else {
					insertToMode(acl.mode, kGroupPermissionsModeOffset, entry.perm);
				}
				break;
			case ACL_GROUP:
				validateIdEntry(acl, entry);
				prepareExtendedAcl(acl);
				acl.extendedAcl->addNamedGroup(entry.id, entry.perm);
				break;
			case ACL_MASK:
				validateNonIdEntry(appearedTagsBitmask, entry);
				prepareExtendedAcl(acl); // Move owning group permissions to eacl if necessary
				insertToMode(acl.mode, kGroupPermissionsModeOffset, entry.perm);
				break;
			case ACL_OTHER:
				validateNonIdEntry(appearedTagsBitmask, entry);
				insertToMode(acl.mode, kOtherPermissionsModeOffset, entry.perm);
				break;
			default:
				throw ConversionErrorException("Unknown ACL xattr entry tag");
		}
		appearedTagsBitmask |= entry.tag;
	}

	// Check if at least minimal ACL appeared
	if ((appearedTagsBitmask & (ACL_USER_OBJ | ACL_GROUP_OBJ | ACL_OTHER)) !=
			(ACL_USER_OBJ | ACL_GROUP_OBJ | ACL_OTHER)) {
		throw ConversionErrorException("ACL xattr without all minimal ACL entries");
	}
	// Extended ACL without mask is invalid
	if ((appearedTagsBitmask & (ACL_GROUP | ACL_USER)) && !(appearedTagsBitmask & ACL_MASK)) {
		throw ConversionErrorException("Extended ACL without permissions mask");
	}

	return acl;
}

std::vector<uint8_t> aclConverter::aclObjectToXattr(const AccessControlList& acl) {
	// NOTE Documentation says nothing about the order of entries. It's supposed
	//      to be ascending by a tag value, so we implement it.
	PosixAclXattr xattr;
	xattr.version = POSIX_ACL_XATTR_VERSION;

	// Owner user permissions
	xattr.entries.push_back(
			{ACL_USER_OBJ, extractMask(acl.mode, kUserPermissionsModeOffset), ACL_UNDEFINED_ID});

	if (acl.extendedAcl) {
		// Users permissions
		for (ExtendedAcl::Entry entry : acl.extendedAcl->list()) {
			if (entry.type != ExtendedAcl::EntryType::kNamedUser) {
				continue;
			}
			xattr.entries.push_back(
					{ACL_USER, assertPermissionsOnTheFly(entry.mask), entry.id});
		}

		// Owning group permissions from ExtendedAcl
		xattr.entries.push_back({ACL_GROUP_OBJ,
				assertPermissionsOnTheFly(acl.extendedAcl->owningGroupMask()), ACL_UNDEFINED_ID});

		// Groups permissions
		for (ExtendedAcl::Entry entry : acl.extendedAcl->list()) {
			if (entry.type != ExtendedAcl::EntryType::kNamedGroup) {
				continue;
			}
			xattr.entries.push_back(
					{ACL_GROUP, assertPermissionsOnTheFly(entry.mask), entry.id});
		}

		// Permissions mask
		xattr.entries.push_back(
				{ACL_MASK, extractMask(acl.mode, kGroupPermissionsModeOffset), ACL_UNDEFINED_ID});
	} else {
		// Owning group permissions from mode
		xattr.entries.push_back({ACL_GROUP_OBJ,
				extractMask(acl.mode, kGroupPermissionsModeOffset), ACL_UNDEFINED_ID});
	}

	// Other permissions
	xattr.entries.push_back(
			{ACL_OTHER, extractMask(acl.mode, kOtherPermissionsModeOffset), ACL_UNDEFINED_ID});

	// Write to buffer
	std::vector<uint8_t> buffer(xattr.rawSize());
	size_t writtenSize = xattr.write(buffer.data());
	if (writtenSize != buffer.size()) {
		throw ConversionErrorException("xattr data incorrectly written to a buffer");
	}
	return buffer;
}
