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
 */

#include "common/platform.h"
#include "common/richacl.h"

#include <gtest/gtest.h>

#define RICHACL_FULL_MASK (RichACL::Ace::kReadData | RichACL::Ace::kWriteData \
	| RichACL::Ace::kAppendData | RichACL::Ace::kReadNamedAttrs \
	| RichACL::Ace::kWriteNamedAttrs |  RichACL::Ace::kExecute | RichACL::Ace::kDeleteChild \
	| RichACL::Ace::kReadAttributes | RichACL::Ace::kWriteAttributes \
	| RichACL::Ace::kWriteRetention | RichACL::Ace::kWriteRetentionHold \
	| RichACL::Ace::kDelete | RichACL::Ace::kReadAcl | RichACL::Ace::kWriteAcl \
	| RichACL::Ace::kWriteOwner | RichACL::Ace::kSynchronize)

TEST(RichACL, CorrectString) {
	RichACL acl;

	acl.setFlags(RichACL::kAutoInherit);
	acl.setOwnerMask(RichACL::Ace::kDeleteChild | RichACL::Ace::kListDirectory);
	acl.setGroupMask(0);
	acl.setOtherMask(RICHACL_FULL_MASK);

	RichACL::Ace ace(
		RichACL::Ace::kAccessAllowedAceType,
		0,
		RichACL::Ace::kAppendData | RichACL::Ace::kWriteAcl,
		17
	);
	acl.insert(ace);
	ace.mask |= RichACL::Ace::kExecute;
	ace.flags |= RichACL::Ace::kIdentifierGroup;
	acl.insert(ace);
	ace.flags |= RichACL::Ace::kSpecialWho;
	ace.flags &= ~RichACL::Ace::kIdentifierGroup;
	ace.id = RichACL::Ace::kEveryoneSpecialId;
	ace.type = RichACL::Ace::kAccessDeniedAceType;
	acl.insert(ace);

	std::string repr = acl.toString();
	std::string model = "a|rd||rwpxdDaARWcCoSeE|pC::A:u17/pxC::A:g17/pxC::D:E/";
	ASSERT_EQ(repr, model);

	RichACL converted = RichACL::fromString(repr);
	ASSERT_EQ(acl, converted);
	ASSERT_EQ(acl.toString(), converted.toString());

	ace.id = RichACL::Ace::kGroupSpecialId;
	converted.insert(ace);
	std::string model_unsorted = "a|rd||eWpxdDaARwcCoSrE|Cp::A:u17/pxC::A:g17/Cxp::D:E/pxC::D:G/";
	model = "a|rd||rwpxdDaARWcCoSeE|pC::A:u17/pxC::A:g17/pxC::D:E/pxC::D:G/";
	ASSERT_EQ(converted.toString(), model);
	ASSERT_EQ(converted, RichACL::fromString(converted.toString()));
	ASSERT_EQ(model, RichACL::fromString(model_unsorted).toString());
}

TEST(RichACL, IncorrectString) {
	std::vector<std::string> wrongs = {
		"",
		"a||",
		"a|rd|||rwpxdDaARWcCoSeE|pC::A:u17/pxC::A:g17/pxC::D:E/pxC::D:G/",
		"abc",
		"a|rd||rwpxdDaARWzCoSeE|pC::A:u17/pxC::A:g17/pxC::D:E/pxC::D:G/",
		"z|rd||rwpxdDaARWcCoSeE|pC::A:u17/pxC::A:g17/pxC::D:E/pxC::D:G/",
		"a|rd||rwpxdDaARWcCoSeE|pC::A:u17/pxC::A:g17/pxC::D:E/pxC::D:Z/",
		"a|rd||rwpxdDaARWcCoSeE|pC::A:u17/pxC::Z:g17/pxC::D:E/pxC::D:G/"
		"a|rdrwpxdDaARWcCoSeE|pC::A:u17/pxC::A:g17/pxC::D:E/pxC::D:G/",
		"/",
	};
	for (const auto &wrong : wrongs) {
		ASSERT_THROW(RichACL::fromString(wrong), RichACL::FormatException) << wrong;
	}
}
