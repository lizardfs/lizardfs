#pragma once

#include "common/platform.h"

namespace metadataserver {

enum class Personality {
	kMaster,
	kShadow
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

/*! \brief Initialize personality subsystem.
 */
int personality_init();

/*! \brief Load personality settings from config file.
 *
 * \return Current (newly loaded) metadata server personality.
 */
Personality loadPersonality();

/*! \brief Tell if metadata server is in transition from Shadow to Master peronality.
 *
 * \return True iff metadata server is in transition from Shadow to Master peronality.
 */
bool isDuringPersonalityChange();

/*! \brief Tell if current metadata server peronality is Master.
 *
 * \return True iff current metadata server peronality is Master.
 */
bool isMaster();

}

