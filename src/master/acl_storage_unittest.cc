/*
   Copyright 2017 Skytechnology sp. z o.o..

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

#include "master/acl_storage.h"

#include <gtest/gtest.h>

TEST(AclStorageTests, Basic) {
	AclStorage storage;
	RichACL acl;
	acl.setMode(0755, false);

	storage.set(1, RichACL(acl));
	storage.set(2, RichACL(acl));

	const auto p_acl = storage.get(1);
	ASSERT_EQ(acl, *p_acl);
	ASSERT_EQ(p_acl, storage.get(1));
	ASSERT_EQ(p_acl, storage.get(2));

	RichACL first = *storage.get(1);
	storage.set(3, std::move(first));
	storage.erase(1);
	ASSERT_FALSE(storage.get(1));
	ASSERT_EQ(p_acl, storage.get(2));

	storage.setMode(1, 0357, false);
	ASSERT_FALSE(storage.get(1));
	ASSERT_EQ(p_acl, storage.get(2));

	storage.setMode(2, 0357, false);
	ASSERT_FALSE(storage.get(1));

	acl.setMode(0357, false);
	ASSERT_NE(acl, *p_acl);
	ASSERT_EQ(acl, *storage.get(2));
}
