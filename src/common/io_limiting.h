#pragma once

#include "config.h"

#include <condition_variable>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <vector>

#include "common/io_limit_group.h"
#include "common/io_limits_database.h"
#include "common/time_utils.h"

namespace ioLimiting {

/**
 * High level I/O limiter class. It enables one to configure a limiting by registering a
 * reconfiguration function, calling it and request a resource assignment. A limiter can be
 * reconfigured many times.
 *
 * This is an ABC. As for now it can represent an I/O limiter running locally in mount,
 * remotely in master, locally in tests etc.
 */
struct Limiter {
	// request bandwidth allocation and return obtained result (lower or equal to 'size')
	virtual uint64_t request(const IoLimitGroupId& groupId, uint64_t size) = 0;

	// Type of a function that will be called to handle a reconfiguration
	typedef std::function<void (
			uint32_t /* delta */,
			const std::string& /* subsystem */,
			const std::vector<IoLimitGroupId>&  /* valid groups */)
	> ReconfigurationFunction;

	// register reconfiguration callback
	void registerReconfigure(ReconfigurationFunction);
	virtual ~Limiter() {}
protected:
	ReconfigurationFunction reconfigure_;
};

// Abstract clock used by the limiting mechanism, introduced mostly for testing purposed.
// Should be monotonic.
struct Clock {
	// A method that returns a current time of a clock
	virtual SteadyTimePoint now() = 0;
	// A method that sleeps until now() > 'time'
	virtual void sleepUntil(SteadyTimePoint time) = 0;
	virtual ~Clock() {}
};

// Real time clock
struct RTClock : public Clock {
	SteadyTimePoint now() override;
	void sleepUntil(SteadyTimePoint time) override;
};

// State shared by 'Group' instances.
struct SharedState {
	SharedState(Limiter& limiter, std::chrono::microseconds delta) :
		limiter(limiter), delta(delta) {}
	// A limiter that is used (local or remote)
	Limiter& limiter;
	// If a user of a group requests a resource assignment and its request
	// isn't fully satisfied, it should not send another request sooner then
	// after 'delta' microseconds:
	std::chrono::microseconds delta;
};

// Single IO limiting group, allowing users to wait for a resource assignment
class Group {
public:
	Group(const SharedState& shared, const std::string& groupId, Clock& clock) : shared_(shared),
		groupId_(groupId), reserve_(0), lastRequestSuccessful_(true), dead_(false), clock_(clock) {}
	virtual ~Group() {}

	// wait until we are allowed to transfer size bytes, return errno-style code
	uint8_t wait(uint64_t size, const SteadyTimePoint deadline, std::unique_lock<std::mutex>& lock);
	// notify all waitees that the group has been removed
	void die();
private:
	// we keep some information about past and pending requests in order to calculate
	// suitable size for bandwidth reservations
	struct PastRequest {
		PastRequest(SteadyTimePoint creationTime, uint64_t size)
			: creationTime(creationTime), size(size) { }
		SteadyTimePoint creationTime;
		uint64_t size;
	};
	struct PendingRequest {
		PendingRequest(uint64_t size) : size(size) {}
		std::condition_variable cond;
		uint64_t size;
	};
	typedef std::list<PastRequest> PastRequests;
	typedef std::list<PendingRequest> PendingRequests;

	PendingRequests::iterator enqueue(uint64_t size);
	void dequeue(PendingRequests::iterator it);
	bool isFirst(PendingRequests::iterator) const;
	bool attempt(uint64_t size);
	void askMaster(std::unique_lock<std::mutex>& lock);
	void notifyQueue();

	const SharedState& shared_;
	const std::string groupId_;
	PastRequests pastRequests_;
	PendingRequests pendingRequests_;
	uint64_t reserve_;
	// We keep start time of the last sent to master request in order not to communicate with
	// the master to often.
	SteadyTimePoint lastRequestStartTime_;
	// We also keep the timestamp of the end of the last communication with master, in order not
	// to use a bandwidth that we obtained a long time ago. Note that due to the fact that the
	// communication with master can be slow, we can't used start time for this purpose. Note also
	// that we neither can resign from using start time for purposes described above, due to the
	// fact that some decisions are made on a basis of its value before the communication starts.
	SteadyTimePoint lastRequestEndTime_;
	bool lastRequestSuccessful_;
	bool dead_;
	Clock& clock_;
};

} // namespace ioLimiting
