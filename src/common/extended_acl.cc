#include "common/extended_acl.h"

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
