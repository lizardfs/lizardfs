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

#include "common/exception.h"
#include "common/richacl.h"
#include "mount/lizard_client.h"

// NOTICE(sarna): OSX-specific, it's not libacl, even though it looks like it
#include <sys/acl.h>

namespace osxAclConverter {

LIZARDFS_CREATE_EXCEPTION_CLASS(AclConversionException, Exception);

RichACL extractAclObject(const void *data, size_t /*size*/);

std::vector<uint8_t> objectToOsxXattr(const RichACL &acl);

} // namespace osxAclConverter
