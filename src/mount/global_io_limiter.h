#pragma once

#include "config.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "common/io_limits_database.h"
#include "common/io_limit_group.h"
#include "common/time_utils.h"
#include "mount/mastercomm.h"

namespace ioLimiting {

/**
 * High level I/O limiter class. It enables one to configure a limiting by registering a
 * reconfiguration function, calling it and request a resource assignment. A limiter can be
 * reconfigured many times.
 *
 * This is an ABC. As for now it can represent an I/O limiter running locally in mount,
 * remotely in master or locally in tests.
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

// An object used locally in mount that communicates with a global limiter
// running in master
struct MasterLimiter : public Limiter {
	MasterLimiter();
	~MasterLimiter();
	// Passes a request to master
	uint64_t request(const IoLimitGroupId& groupId, uint64_t size) override;
private:
	class IolimitsConfigHandler : public PacketHandler {
	public:
		IolimitsConfigHandler(MasterLimiter& parent) : parent_(parent) {}

		bool handle(MessageBuffer buffer);
	private:
		MasterLimiter& parent_;
	};

	IolimitsConfigHandler iolimitsConfigHandler_;
	uint32_t configVersion_;
};

// The local limiter running in this mount instance (used in local I/O limiting)
struct MountLimiter : public Limiter {
	uint64_t request(const IoLimitGroupId& groupId, uint64_t size) override;
	void loadConfiguration(const IoLimitsConfigLoader& config);
private:
	IoLimitsDatabase database_;
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
		groupId_(groupId), reserve_(0), outstandingRequest_(false), lastRequestSuccessful_(true),
		dead_(false), clock_(clock) {}
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
	bool canAskMaster() const;
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
	bool outstandingRequest_;
	bool lastRequestSuccessful_;
	bool dead_;
	Clock& clock_;
};

// This class is a proxy that locally handles calls to a possibly remote Limiter.
// It classifies clients into groups and performs required delays.
class LimiterProxy {
public:
	LimiterProxy(Limiter& limiter, Clock& clock) :
		shared_(limiter, std::chrono::milliseconds(100)),
		enabled_(true),
		clock_(clock)
	{
		using namespace std::placeholders;
		limiter.registerReconfigure(std::bind(&LimiterProxy::reconfigure, this, _1, _2, _3));
	}

	// Try to acquire an assignment of 'size' bytes. This method pauses a callee until a request
	// is satisfied or a deadline is exceeded. Return when returns errno-style code
	uint8_t waitForRead(const pid_t pid, const uint64_t size, SteadyTimePoint deadline);
	// Works the same as waitForRead
	uint8_t waitForWrite(const pid_t pid, const uint64_t size, SteadyTimePoint deadline);

private:
	typedef std::map<IoLimitGroupId, std::shared_ptr<Group>> Groups;

	std::shared_ptr<Group> getGroup(const IoLimitGroupId& groupId) const;
	// Remove groups that were deleted, cancel queued operations assigned to them. Add new groups.
	// Update the delta_us parameter.
	// If subsystem was changed, cancel all queued operations and removed groups that were used.
	void reconfigure(uint32_t delta_us, const std::string& subsystem,
			const std::vector<IoLimitGroupId>& groupIds);

	std::mutex mutex_;
	SharedState shared_;
	std::string subsystem_;
	Groups groups_;
	bool enabled_;
	Clock& clock_;
};

} // namespace ioLimiting
