/*
   Copyright 2016 Skytechnology sp. z o.o.

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

/*! \brief Implementation of class for storing and executing tasks
 *
 * This class is responsible for managing execution of tasks.
 * Submitting a task creates a new Job object. Job represents
 * the task itself + all subtasks it creates during its execution.
 */
class TaskManager {
public:
	/*! \brief Class representing a single task to be executed by Task Manager*/
	class Task {
	public:
		virtual ~Task() {
		}
		/*! \brief Pure virtual function that represents execution of task.
		 * \param ts current time stamp.
		 * \param work_queue a list to which this task adds newly created tasks.
		 */
		virtual int execute(uint32_t ts, std::list<std::unique_ptr<Task>> &work_queue) = 0;

		virtual bool isFinished() const = 0;
	};

	typedef typename std::list<std::unique_ptr<Task>>::iterator TaskIterator;

	/*! \brief Class representing the original task and all subtasks it created during execution*/
	class Job {
	public:
		Job() : finish_callback_(), tasks_() {
		}

		Job(Job &&other) : finish_callback_(std::move(other.finish_callback_)),
				   tasks_(std::move(other.tasks_)) {
		}

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

		void push_back(std::unique_ptr<Task> &&task) {
			tasks_.push_back(std::move(task));
		}

	private:
		std::function<void(int)> finish_callback_; /*!< Callback function called when all tasks
		                                                that belong to this Job are done. */

		std::list<std::unique_ptr<Task>> tasks_;   /*!< List of tasks that belong to this Job*/
	};

	typedef typename std::list<Job> JobContainer;
	typedef typename JobContainer::iterator JobIterator;

public:
	TaskManager() : job_list_() {
	}

	/*! \brief Submit task to be enqueued and executed by TaskManager.
	 *
	 * Submitting task creates Job object which contains data of original
	 * task and all tasks that were created during execution.
	 * \param ts current time stamp.
	 * \param initial_batch_size initial number of tasks to be processed
	 *                           before putting the Job on the list
	 * \param task Task to be submitted.
	 * \param callback Callback funtion that will be called when all the
	 *                 tasks related to this sumbmission are finished.
	 *                 This function is called only if submitted work is not
	 *                 finished after completing the 'initial_batch_size'
	 *                 number of tasks.
	 * \return Value representing the status.
	 */
	int submitTask(uint32_t ts, int initial_batch_size, std::unique_ptr<Task> &&task,
		       const std::function<void(int)> &callback);

	/*! \brief Iterate over Jobs and execute tasks.
	 *
	 * This function goes through the list of Jobs over and over again,
	 * executing one task each time it processes a Job.
	 * \param ts current time stamp.
	 * \param number_of_tasks maximum number of tasks to be processed.
	 */
	void processJobs(uint32_t ts, int number_of_tasks);

	bool workAvailable() const {
		return !job_list_.empty();
	}

private:
	JobContainer job_list_; /*!< List with Jobs to execute. */
};
