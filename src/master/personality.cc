#include "common/platform.h"
#include "personality.h"

#include <algorithm>

#include "common/cfg.h"
#include "common/exceptions.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/slogger.h"

namespace metadataserver {

static Personality gPersonality = Personality::kMaster;

Personality getPersonality() {
	return gPersonality;
}

void setPersonality(Personality personality) {
	gPersonality = personality;
}

bool personalityInConfigIsAuto() {
	const std::string kAuto = "ha-cluster-managed";
	std::string p = cfg_get("PERSONALITY", "not ha-cluster-managed");
	std::transform(p.begin(), p.end(), p.begin(), tolower);
	return p == kAuto;
}

Personality loadNonHaClusterPersonality() {
	const std::string kMaster = "master";
	const std::string kShadow = "shadow";
	std::string p = cfg_get("PERSONALITY", kMaster);
	std::transform(p.begin(), p.end(), p.begin(), tolower);
	if (p == kMaster) {
		return Personality::kMaster;
	} else if (p == kShadow) {
		return Personality::kShadow;
	} else if (personalityInConfigIsAuto()) {
		return gPersonality;
	} else {
		throw ConfigurationException("bad personality");
	}
}

static std::vector<void(*)(void)> changePersonalityReloadFunctions;

void registerFunctionCalledOnPromotion(void(*f)(void)) {
	changePersonalityReloadFunctions.push_back(f);
}

void promoteToMaster() {
	lzfs_pretty_syslog(LOG_INFO, "changing metadataserver personality from Shadow to Master");
	for (auto& f : changePersonalityReloadFunctions) {
		f();
	}
	setPersonality(Personality::kMaster);
}

void personality_reload(void) {
	try {
		Personality personality = loadNonHaClusterPersonality();
		if (personality != gPersonality) {
			if (personality == Personality::kMaster) {
				promoteToMaster();
			} else {
				lzfs_pretty_syslog(LOG_ERR, "trying to preform forbidden personality change from Master to Shadow");
			}
		}
	} catch (const ConfigurationException& e) {
		/* reload shall not break instance */
		lzfs_pretty_syslog(LOG_ERR, "bad configuration: `%s'", e.what());
	}
}

bool promoteAutoToMaster() {
	if (!personalityInConfigIsAuto()) {
		return false;
	}
	if (gPersonality != Personality::kShadow) {
		return false;
	}
	promoteToMaster();
	return true;
}

int personality_init() {
	int ret = 0;
	try {
		if (personalityInConfigIsAuto()) {
			for (auto option : main_get_extra_arguments()) {
				std::transform(option.begin(), option.end(), option.begin(), tolower);
				if (option == "ha-cluster-personality=master") {
					setPersonality(Personality::kMaster);
					return 0;
				} else if (option == "ha-cluster-personality=shadow") {
					setPersonality(Personality::kShadow);
					return 0;
				}
			}
			throw ConfigurationException(
					"Personality 'ha-cluster-managed' should only be used by HA cluster");
		} else {
			setPersonality(loadNonHaClusterPersonality());
		}
	} catch (const ConfigurationException& e) {
		ret = -1;
	}
#ifndef METARESTORE
	main_reloadregister(personality_reload);
#endif /* #ifndef METARESTORE */
	return ret;
}

bool isMaster() {
	return gPersonality == Personality::kMaster;
}

}

