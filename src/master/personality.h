#pragma once

#include "config.h"

namespace master {

enum class Personality {
	kMaster,
	kShadow
};

Personality getPersonality();
void setPersonality(Personality);

}

