#pragma once

#include "common/platform.h"

#include "common/time_utils.h"

/**
 * TokenBucket is a rate limiter. It distributes 'budget_' resources among clients.
 * Every nanosecond the available limit grows by 'rate_/(10^9)' resources, but only
 * up to 'budgetCeil_' limit.
 *
 * If a client tries to reserve more resources than are available, it will be assigned
 * everything that is currently available.
 */
class TokenBucket {
public:
	TokenBucket(SteadyTimePoint now) : rate_(0), budget_(0), budgetCeil_(0), prevTime_(now) {}

	// set rate, ceil and (optionally) budget
	void reconfigure(SteadyTimePoint now, double rate, double budgetCeil);
	void reconfigure(SteadyTimePoint now, double rate, double budgetCeil, double budget);

	// getters
	double rate() const;
	double budgetCeil() const;

	// Try to satisfy the request. Return 'cost' or less, but not less than 0. Require cost > 0
	double attempt(SteadyTimePoint now, double cost);

private:
	void updateBudget(SteadyTimePoint now);
	void verifyClockSteadiness(SteadyTimePoint now);

	double rate_;
	double budget_;
	double budgetCeil_;

	SteadyTimePoint prevTime_;
};
