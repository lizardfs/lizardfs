/*
   Copyright 2016-2017 Skytechnology sp. z o.o.

   This file is part of LizardFS.

   LizardFS is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, version 3.

   LizardFS is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with LizardFS. If not, see <http://www.gnu.org/licenses/>.
*/

#pragma once

#include "common/platform.h"

#include <functional>
#include <list>
#include <memory>
#include <string>

#include "common/intrusive_list.h"
#include "common/job_info.h"

/*! \brief Implementation of class for storing and executing tasks
 *
 * This class is responsible for managing execution of tasks.
 * Submitting a task creates a new Job object. Job represents
 * the task itself + all subtasks it creates during its execution.
 */
class TaskManager {
public:
	/*! \brief Class representing a single task to be executed by Task Manager*/
	class Task : public intrusive_list_base_hook {
	public:
		virtual ~Task() {
		}
		/*! \brief Pure virtual function that represents execution of task.
		 * \param ts current time stamp.
		 * \param work_queue a list to which this task adds newly created tasks.
		 */
		virtual int execute(uint32_t ts, intrusive_list<Task> &work_queue) = 0;

		virtual bool isFinished() const = 0;
	};

	typedef typename intrusive_list<Task>::iterator TaskIterator;

	/*! \brief Class representing the original task and all subtasks it created during execution*/
	class Job {
	public:
		Job(uint32_t id, const std::string &description) :
		    id_(id), description_(description),
		    finish_callback_(), tasks_() {
		}

		Job(Job &&other) : id_(std::move(other.id_)),
				   description_(std::move(other.description_)),
				   finish_callback_(std::move(other.finish_callback_)),
				   tasks_(std::move(other.tasks_)) {
		}

		~Job() {
			tasks_.clear_and_dispose([](Task *ptr) { delete ptr; });
		}

		void finalize(int status);

		/*! \brief Function finalizes processing of single task.
		 * \param itask iterator to a task that was executed.
		 * \param status indicates whether task execution was successful.
		 */
		void finalizeTask(TaskIterator itask, int status);

		/*! \brief Function executes and finalizes the first task from the list of tasks.
		 * \param ts current time stamp.
		 */
		void processTask(uint32_t ts);

		void setFinishCallback(const std::function<void(int)> &finish_callback) {
			finish_callback_ = finish_callback;
		}

		void setFinishCallback(std::function<void(int)> &&finish_callback) {
			finish_callback_ = std::move(finish_callback);
		}

		bool isFinished() const {
			return tasks_.empty();
		}

		void addTask(Task *task) {
			tasks_.push_back(*task);
		}

		uint32_t getId() {
			return id_;
		}

		JobInfo getInfo() const;

	private:
		uint32_t id_;
		std::string description_;
		std::function<void(int)> finish_callback_; /*!< Callback function called when all tasks
		                                                that belong to this Job are done. */

		intrusive_list<Task> tasks_; /*!< List of tasks that belong to this Job*/
	};

	typedef typename std::list<Job> JobContainer;
	typedef typename JobContainer::iterator JobIterator;
	typedef typename std::vector<JobInfo> JobsInfoContainer;

public:
	TaskManager() : job_list_(), next_job_id_(0) {
	}

	/*! \brief Submit task to be enqueued and executed by TaskManager.
	 *
	 * Submitting task creates Job object which contains data of original
	 * task and all tasks that were created during execution.
	 * \param job_id id of the created Job
	 * \param ts current time stamp.
	 * \param initial_batch_size initial number of tasks to be processed
	 *                           before putting the Job on the list
	 * \param task Task to be submitted.
	 * \param callback Callback function that will be called when all the
	 *                 tasks related to this sumbmission are finished.
	 *                 This function is called only if submitted work is not
	 *                 finished after completing the 'initial_batch_size'
	 *                 number of tasks.
	 * \return Value representing the status.
	 */
	int submitTask(uint32_t job_id, uint32_t ts, int initial_batch_size, Task *task,
		       const std::string &description, const std::function<void(int)> &callback);

	/*! \brief Submit task to be enqueued and executed by TaskManager.
	 *
	 * Calls submitTask declared above without specifying job_id.
	 * This version is used by Tasks which do not support cancelling
	 * their execution.
	 */
	int submitTask(uint32_t ts, int initial_batch_size, Task *task,
		       const std::string &description, const std::function<void(int)> &callback);

	/*! \brief Iterate over Jobs and execute tasks.
	 *
	 * This function goes through the list of Jobs over and over again,
	 * executing one task each time it processes a Job.
	 * \param ts current time stamp.
	 * \param number_of_tasks maximum number of tasks to be processed.
	 */
	void processJobs(uint32_t ts, int number_of_tasks);

	/*! \brief Get information about all currently executed Job. */
	JobsInfoContainer getCurrentJobsInfo() const;

	/*! \brief Stop execution of a Job specified by given id. */
	bool cancelJob(uint32_t job_id);

	bool workAvailable() const {
		return !job_list_.empty();
	}

	uint32_t reserveJobId() {
		return next_job_id_++;
	}
private:
	JobContainer job_list_; /*!< List with Jobs to execute. */
	uint32_t next_job_id_;
};
