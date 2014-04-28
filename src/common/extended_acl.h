#pragma once

#include <cstdint>
#include <vector>

#include "common/massert.h"
#include "common/serialization_macros.h"

class ExtendedAcl {
public:
	/*
	 *  Type for storing three bits (rwx)
	 */
	typedef uint8_t AccessMask;

	/*
	 * Type of entry in the access control list
	 */
	enum class EntryType : uint8_t { kNamedUser, kNamedGroup };

	/*
	 * Entry in the access control list -- uid/gid + access mask
	 */
	struct Entry {
		Entry() {}
		Entry(uint32_t id, EntryType type, AccessMask mask) : id(id), type(type), mask(mask) {}

		bool operator==(const Entry& other) const;
		uint32_t serializedSize() const;
		void serialize(uint8_t** destination) const;
		void deserialize(const uint8_t** source, uint32_t& bytesLeftInBuffer);

		uint32_t id;
		EntryType type;
		AccessMask mask;
	};

	/*
	 * Default constructor just to make life (eg. deserialization,
	 * using std::map<foo, ExtendedAcl>) easier.
	 * Creates an uninitialized object which can be deserialized or assigned to.
	 */
	ExtendedAcl() : owningGroupMask_(0) {}

	explicit ExtendedAcl(AccessMask owningGroupMask) : owningGroupMask_(owningGroupMask) {
		sassert(isAccessMaskValid(owningGroupMask));
	}

	void setOwningGroupMask(AccessMask owningGroupMask) {
		owningGroupMask_ = owningGroupMask;
	}

	AccessMask owningGroupMask() const {
		return owningGroupMask_;
	}

	const std::vector<Entry> list() const {
		return list_;
	}

	void addNamedGroup(uint32_t gid, AccessMask mask) {
		sassert(isAccessMaskValid(mask));
		sassert(!hasEntryFor(EntryType::kNamedGroup, gid));
		list_.push_back(Entry(gid, EntryType::kNamedGroup, mask));
	}

	void addNamedUser(uint32_t uid, AccessMask mask) {
		sassert(isAccessMaskValid(mask));
		sassert(!hasEntryFor(EntryType::kNamedUser, uid));
		list_.push_back(Entry(uid, EntryType::kNamedUser, mask));
	}

	bool hasEntryFor(EntryType type, uint32_t id) const {
		for (const auto& entry : list_) {
			if (entry.type == type && entry.id == id) {
				return true;
			}
		}
		return false;
	}

	bool hasNamedGroup(uint32_t id) const {
		return hasEntryFor(EntryType::kNamedGroup, id);
	}

	bool hasNamedUser(uint32_t id) const {
		return hasEntryFor(EntryType::kNamedUser, id);
	}

	static bool isAccessMaskValid(AccessMask mask) {
		return mask <= 7;
	}

	LIZARDFS_DEFINE_SERIALIZE_METHODS(ExtendedAcl,
			AccessMask, owningGroupMask_,
			std::vector<Entry>, list_);

private:
	AccessMask owningGroupMask_;
	std::vector<Entry> list_;
};
