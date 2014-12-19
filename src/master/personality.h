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

