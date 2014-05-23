#pragma once

#include "config.h"

#include "common/time_utils.h"

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
