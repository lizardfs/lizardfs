#include "common/platform.h"
#include "common/extended_acl.h"

#include <tuple>

bool ExtendedAcl::Entry::operator==(const ExtendedAcl::Entry& other) const {
	return std::make_tuple(type, id, mask) == std::make_tuple(other.type, other.id, other.mask);
}

uint32_t ExtendedAcl::Entry::serializedSize() const {
	return ::serializedSize(bool(), id, mask);
}

void ExtendedAcl::Entry::serialize(uint8_t** destination) const {
	::serialize(destination, (type == EntryType::kNamedUser), id, mask);
}

void ExtendedAcl::Entry::deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer) {
	bool isNamedUser;
	::deserialize(source, bytesLeftInBuffer, isNamedUser, id, mask);
	type = (isNamedUser ? EntryType::kNamedUser : EntryType::kNamedGroup);
}
