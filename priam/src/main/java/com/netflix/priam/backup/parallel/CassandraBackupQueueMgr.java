/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup.parallel;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractSet;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/*
 * 
 * Represents a queue of files (incrementals, snapshots, meta files) to be uploaded.
 * 
 * Duties of the mgr include:
 * - Mechanism to add a task, including de-duplication of tasks before adding to queue.
 * - Guarantee delivery of task to only one consumer.
 * - Provide relevant metrics including number of tasks in queue, etc.
 */

@Singleton
public class CassandraBackupQueueMgr implements ITaskQueueMgr<AbstractBackupPath> {

    private static final Logger logger = LoggerFactory.getLogger(CassandraBackupQueueMgr.class);

    private BlockingQueue<AbstractBackupPath> tasks; //A queue of files to be uploaded
    private AbstractSet<String> tasksQueued; //A queue to determine what files have been queued, used for deduplication

    @Inject
    public CassandraBackupQueueMgr(IConfiguration config) {
        tasks = new ArrayBlockingQueue<AbstractBackupPath>(config.getIncrementalBkupQueueSize());
        // Key to task is the S3 absolute path (BASE/REGION/CLUSTER/TOKEN/[yyyymmddhhmm]/[SST|SNP|META]/KEYSPACE/COLUMNFAMILY/FILE
        tasksQueued = new HashSet<String>(config.getIncrementalBkupQueueSize());
    }

    @Override
    public void add(AbstractBackupPath task)
    {
        if (!tasksQueued.contains(task.getRemotePath())) {
            tasksQueued.add(task.getRemotePath());
            try {
                // block until space becomes available in queue
                tasks.put(task);
                logger.debug("Queued file {} within CF {}", task.getFileName(), task.getColumnFamily());
            } catch (InterruptedException e) {
                logger.warn("Interrupted waiting for the task queue to have free space, not fatal will just move on.   Error Msg: {}", e.getLocalizedMessage());
                tasksQueued.remove(task.getRemotePath());
            }
        } else {
            logger.debug("Already in queue, no-op.  File: {}", task.getRemotePath());
        }
    }

    @Override
    public AbstractBackupPath take() throws InterruptedException {
        AbstractBackupPath task = null;
        if (!tasks.isEmpty()) {

            synchronized (tasks) {
                task = tasks.poll(); //non-blocking call
            }
        }

        return task;
    }

    @Override
    public Boolean hasTasks() {
        return !tasks.isEmpty();
    }

    @Override
    public void taskPostProcessing(AbstractBackupPath completedTask) {
        this.tasksQueued.remove(completedTask.getRemotePath());
    }

    @Override
    public Integer getNumOfTasksToBeProcessed() {
        return tasks.size();
    }

    @Override
    public Boolean tasksCompleted(Date date) {
        throw new UnsupportedOperationException();
    }

}