/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup.parallel;

/*
 * 
 * Represents a queue of tasks to be completed.
 * 
 * Duties of the mgr include:
 * - Mechanism to add a task, including deduplication of tasks before adding to queue.
 * - Guarantee delivery of task to only one consumer.
 * - Provide relevant metrics including number of tasks in queue, number of tasks processed.
 */
public interface ITaskQueueMgr<E> {

	public void add(E task);
	/*
	 * @return task, null if none is available.
	 */
	public E take() throws InterruptedException;
	/*
	 * @return true if there are tasks within queue to be processed; false otherwise.
	 */
	public Boolean hasTasks();
	
	/*
	 * A means to perform any post processing once the task has been completed.  If post processing is needed,
	 * the consumer should notify this behavior via callback once the task is completed. 
	 * 
	 * *Note: "completed" here can mean success or failure.
	 */
	public void taskPostProcessing(E completedTask);
	
	
	
	public Integer getNumOfTasksToBeProessed();
	/*
	 * @return true if all tasks completed (includes failures) for a date; false, if at least 1 task is still in queue.
	 */
	public Boolean tasksCompleted(java.util.Date date);
}