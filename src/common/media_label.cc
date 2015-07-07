/*
   Copyright 2013-2014 EditShare, 2013-2015 Skytechnology sp. z o.o.

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
#include "common/media_label.h"

const MediaLabel kMediaLabelWildcard = "_";

bool isMediaLabelValid(const MediaLabel& mediaLabel) {
	const uint32_t maxLength = 32;
	if (mediaLabel.empty() || mediaLabel.size() > maxLength) {
		return false;
	}
	for (char c : mediaLabel) {
		if (!(c == '_'
				|| (c >= 'a' && c <= 'z')
				|| (c >= 'A' && c <= 'Z')
				|| (c >= '0' && c <= '9'))) {
			return false;
		}
	}
	return true;
}
