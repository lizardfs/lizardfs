#include "config.h"
#include "personality.h"

namespace master {

static Personality gPersonality;

Personality getPersonality() {
	return gPersonality;
}

void setPersonality(Personality personality) {
	gPersonality = personality;
}

}

