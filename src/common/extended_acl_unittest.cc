#include "common/extended_acl.h"

#include <gtest/gtest.h>

TEST(ExtendedAclTests, OwningGroupMask) {
	EXPECT_EQ(2U, ExtendedAcl(2).owningGroupMask());
	EXPECT_EQ(4U, ExtendedAcl(4).owningGroupMask());
	EXPECT_ANY_THROW(ExtendedAcl(0155));
}

TEST(ExtendedAclTests, AddNamedEntries) {
	ExtendedAcl acl;

	// Add entry r-x for gid 123
	EXPECT_FALSE(acl.hasEntryFor(ExtendedAcl::EntryType::kNamedGroup, 123));
	EXPECT_FALSE(acl.hasEntryFor(ExtendedAcl::EntryType::kNamedUser, 123));
	EXPECT_ANY_THROW(acl.addNamedGroup(123, 0155));
	EXPECT_NO_THROW(acl.addNamedGroup(123, 5));
	EXPECT_TRUE(acl.hasEntryFor(ExtendedAcl::EntryType::kNamedGroup, 123));
	EXPECT_FALSE(acl.hasEntryFor(ExtendedAcl::EntryType::kNamedUser, 123));
	EXPECT_EQ(1U, acl.list().size());
	EXPECT_ANY_THROW(acl.addNamedGroup(123, 5));

	// Add entry rwx for uid 123
	EXPECT_NO_THROW(acl.addNamedUser(123, 7));
	EXPECT_TRUE(acl.hasEntryFor(ExtendedAcl::EntryType::kNamedGroup, 123));
	EXPECT_TRUE(acl.hasEntryFor(ExtendedAcl::EntryType::kNamedUser, 123));
	EXPECT_EQ(2U, acl.list().size());

	// Verify the list
	for (const auto& entry : acl.list()) {
		if (entry.type == ExtendedAcl::EntryType::kNamedGroup) {
			EXPECT_EQ(123, entry.id);
			EXPECT_EQ(5, entry.mask);
		} else {
			EXPECT_EQ(ExtendedAcl::EntryType::kNamedUser, entry.type);
			EXPECT_EQ(123, entry.id);
			EXPECT_EQ(7, entry.mask);
		}
	}
}

