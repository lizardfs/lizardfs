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

#pragma once

#include "common/platform.h"

namespace metadataserver {

enum class Personality {
	kMaster,
	kShadow,
};

/*! \brief Get current personality.
 *
 * \return Current metadata server personality.
 */
Personality getPersonality();

/*! \brief Set new personality for metadata server.
 *
 * One can use this function to change personality from Shadow to Master.
 * Changing persolality from Master to Shadow is forbidden.
 *
 * \param personality - new personality for metadata server.
 */
void setPersonality(Personality personality);

/*! \brief Validate if personality from config and command line match each other
 *
 * \return 0 if config is valid, throws otherwise
 */
int personality_validate();

/*! \brief Initialize personality subsystem.
 */
int personality_init();

/*! \brief Tell if current metadata server peronality is Master.
 *
 * \return True iff current metadata server peronality is Master.
 */
bool isMaster();

/*! \brief Register functions that will be called on metadata server promotion
 */
void registerFunctionCalledOnPromotion(void(*)(void));

/*! \brief Changes personality from AUTO to MASTER.
 *
 * \return True if successfully changed personality from AUTO to MASTER
 */
bool promoteAutoToMaster();

}

