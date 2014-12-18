#include "common/platform.h"
#include "personality.h"

#include <algorithm>

#include "common/cfg.h"
#include "common/exceptions.h"
#include "common/main.h"
#include "common/massert.h"
#include "common/slogger.h"

namespace metadataserver {

static const std::string kMasterCmdOption = "initial-personality=master";
static const std::string kShadowCmdOption = "initial-personality=shadow";
static const std::string kClusterManagedCmdOption = "ha-cluster-managed";

static Personality gPersonality = Personality::kMaster;

Personality getPersonality() {
	return gPersonality;
}

void setPersonality(Personality personality) {
	gPersonality = personality;
}

bool personalityInConfigIsAuto() {
	const std::string kAuto = kClusterManagedCmdOption;
	std::string p = cfg_get("PERSONALITY", "not " + kClusterManagedCmdOption);
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

int personality_validate() {
	static std::string haAdvise = "This installation is managed by HA cluster,"
			" one should manipulate metadata servers only using lizardfs-cluster-manager.";
	static std::string nonHaAdvise = "Metadata server configuration states that this installation"
			" is NOT managed by HA cluster. In case if it is supposed to be managed by a cluster "
			" change the configuration (change the personality defined in " + cfg_filename() +
			" to " + kClusterManagedCmdOption + "), otherwise stop using "
			+ kClusterManagedCmdOption + " command line option.";
	for (auto option : main_get_extra_arguments()) {
		std::transform(option.begin(), option.end(), option.begin(), tolower);
		if (personalityInConfigIsAuto() && option == kClusterManagedCmdOption) {
			return 0;
		}
		if (!personalityInConfigIsAuto() && option == kClusterManagedCmdOption) {
			throw ConfigurationException(nonHaAdvise);
		}
	}
	if (personalityInConfigIsAuto()) {
		throw ConfigurationException(haAdvise);
	} else {
		return 0;
	}
}

int personality_init() {
	if (personalityInConfigIsAuto()) {
		for (auto option : main_get_extra_arguments()) {
			std::transform(option.begin(), option.end(), option.begin(), tolower);
			if (option == kMasterCmdOption) {
				setPersonality(Personality::kMaster);
				return 0;
			} else if (option == kShadowCmdOption) {
				setPersonality(Personality::kShadow);
				return 0;
			}
		}
		throw ConfigurationException(
				"Missing " + kMasterCmdOption + " or " + kShadowCmdOption + " command line option");
	} else {
		setPersonality(loadNonHaClusterPersonality());
	}
#ifndef METARESTORE
	main_reloadregister(personality_reload);
#endif /* #ifndef METARESTORE */
	return 0;
}

bool isMaster() {
	return gPersonality == Personality::kMaster;
}

}

