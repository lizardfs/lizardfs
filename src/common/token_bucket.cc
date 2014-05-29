#include "config.h"
#include "common/token_bucket.h"

#include <algorithm>

#include "common/massert.h"

void TokenBucket::reconfigure(SteadyTimePoint now, double rate, double budgetCeil) {
	updateBudget(now);
	rate_ = rate;
	budgetCeil_ = budgetCeil;
}

void TokenBucket::reconfigure(SteadyTimePoint now, double rate, double budgetCeil, double budget) {
	reconfigure(now, rate, budgetCeil);
	budget_ = budget;
}

double TokenBucket::rate() const {
	return rate_;
}

double TokenBucket::budgetCeil() const {
	return budgetCeil_;
}

double TokenBucket::attempt(SteadyTimePoint now, double cost) {
	sassert(cost > 0);
	updateBudget(now);
	const double result = std::min(cost, budget_);
	budget_ -= result;
	return result;
}

void TokenBucket::updateBudget(SteadyTimePoint now) {
	verifyClockSteadiness(now);
	std::chrono::duration<double, std::nano> time(now - prevTime_);
	prevTime_ = now;
	int64_t time_ns = time.count();
	budget_ += rate_ * time_ns / (1000 * 1000 * 1000.0);
	if (budget_ > budgetCeil_) {
		budget_ = budgetCeil_;
	}
}

void TokenBucket::verifyClockSteadiness(SteadyTimePoint now) {
	sassert(now >= prevTime_);
}
