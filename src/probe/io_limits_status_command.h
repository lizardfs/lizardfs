#pragma once

#include "config.h"

#include "common/matocl_communication.h"
#include "probe/lizardfs_probe_command.h"

/**
 * A command for lizardfs-probe that checks the global I/O limiting status
 */
class IoLimitsStatusCommand : public LizardFsProbeCommand {
public:
	/**
	 * Return name of a command
	 */
	std::string name() const override;

	/**
	 * Print help
	 */
	void usage() const override;

	/**
	 * Get a list of used options with theirs description
	 */
	SupportedOptions supportedOptions() const override;

	/**
	 * Perform a command with given arguments and options
	 * \param options List of options and arguments
	 */
	void run(const Options& options) const override;

protected:
	/**
	 * Print human-friendly output
	 * \param configId Sequence number of current configuration
	 * \param period_us Minimal renegotiation period in microseconds
	 * \param accumulate_ms Maximal time of bandwidth to accumulate in milliseconds
	 * \param subsystem Name of a limited subsystem
	 * \param groupsAndLimits Vector of groups with theirs limits
	 */
	void printStandard(uint32_t configId, uint32_t period_us, uint32_t accumulate_ms,
			const std::string& subsystem,
			const std::vector<IoGroupAndLimit>& groupsAndLimits) const;

	/**
	 * Print parsing-friendly output
	 * \param configId Sequence number of current configuration
	 * \param period_us Minimal renegotiation period in microseconds
	 * \param accumulate_ms Maximal time of bandwidth to accumulate in milliseconds
	 * \param subsystem Name of a limited subsystem
	 * \param groupsAndLimits Vector of groups with theirs limits
	 */
	void printPorcelain(uint32_t configId, uint32_t period_us, uint32_t accumulate_ms,
			const std::string& subsystem,
			const std::vector<IoGroupAndLimit>& groupsAndLimits) const;

	/**
	 * Prepare renegotiation period number to print
	 * \param period_us Period in microseconds
	 * \return a string ready to be printed
	 */
	std::string printPeriod(uint32_t period_us) const;

	/**
	 * Check if I/O limiting is disabled
	 * \param subsystem name of a subsystem
	 * \param groups vector of groups with theirs limits
	 * \return Result
	 */
	bool isLimitingDisabled(const std::string& subsystem,
			const std::vector<IoGroupAndLimit>& groups) const;
};
