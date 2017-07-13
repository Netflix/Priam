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
package com.netflix.priam.backup;

import com.google.inject.Singleton;
import com.netflix.priam.utils.DateUtil;
import com.netflix.priam.utils.MaxSizeHashMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
 * A means to manage metadata for various types of backups (snapshots, incrementals)
 */
@Singleton
public abstract class BackupStatusMgr implements IBackupStatusMgr{

	private static final Logger logger = LoggerFactory.getLogger(BackupStatusMgr.class);
	
	 /*
	  * completed bkups represented by its snapshot name (yyyymmddhhmm)
	  * Note:  A linkedlist was chosen for fastest removal of the oldest backup.
	  */
	Map<String, LinkedList<BackupMetadata>> backupMetadataMap;
	int capacity;

	public BackupStatusMgr(int capacity) {
		this.capacity = capacity;
		// This is to avoid us loading lot of status in memory.
		// We will fetch previous status from backend service, if required.
		backupMetadataMap = new MaxSizeHashMap<>(capacity);
	}

	@Override
	public int getCapacity()
	{
		return capacity;
	}

	@Override
	public Map<String, LinkedList<BackupMetadata>> getAllSnapshotStatus()
	{
		return backupMetadataMap;
	}

	@Override
	public LinkedList<BackupMetadata> locate(Date snapshotDate) {
		return locate(DateUtil.formatyyyyMMdd(snapshotDate));
	}

	@Override
	public LinkedList<BackupMetadata> locate(String snapshotDate) {
		if (StringUtils.isEmpty(snapshotDate))
			return null;

		// See if in memory
		if (backupMetadataMap.containsKey(snapshotDate))
			return backupMetadataMap.get(snapshotDate);

		LinkedList<BackupMetadata> metadataLinkedList = fetch(snapshotDate);

		//Save the result in local cache so we don't hit data store/file.
		backupMetadataMap.put(snapshotDate, metadataLinkedList);

		return metadataLinkedList;
	}

	@Override
	public void start(BackupMetadata backupMetadata) {
		LinkedList<BackupMetadata> metadataLinkedList = locate(backupMetadata.getSnapshotDate());

		if (metadataLinkedList == null)
		{
			metadataLinkedList = new LinkedList<>();
		}

		metadataLinkedList.addFirst(backupMetadata);
		backupMetadataMap.put(backupMetadata.getSnapshotDate(), metadataLinkedList);

		//Save the backupMetaDataMap
		save(backupMetadata);
	}

	@Override
	public void finish(BackupMetadata backupMetadata) {
		//validate that it has actually finished. If not, then set the status and current date.
		if (backupMetadata.getStatus() != BackupMetadata.Status.FINISHED)
			backupMetadata.setStatus(BackupMetadata.Status.FINISHED);

		if (backupMetadata.getCompleted() == null)
			backupMetadata.setCompleted(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime());

		//Retrieve the snapshot metadata and then update the finish date/status.
		retrieveAndUpdate(backupMetadata);

		//Save the backupMetaDataMap
		save(backupMetadata);

	}

	private void retrieveAndUpdate(final BackupMetadata backupMetadata)
	{
		//Retrieve the snapshot metadata and then update the date/status.
		LinkedList<BackupMetadata> metadataLinkedList = locate(backupMetadata.getSnapshotDate());

		if (metadataLinkedList == null || metadataLinkedList.isEmpty()) {
			logger.error("No previous backupMetaData found. This should not happen. Creating new to ensure app keeps running.");
			metadataLinkedList = new LinkedList<>();
			metadataLinkedList.addFirst(backupMetadata);
		}

		metadataLinkedList.forEach(backupMetadata1 -> {
			if (backupMetadata1.equals(backupMetadata)){
				backupMetadata1.setCompleted(backupMetadata.getCompleted());
				backupMetadata1.setStatus(backupMetadata.getStatus());
				return;
			}
		});
	}

	@Override
	public void failed(BackupMetadata backupMetadata) {
		//validate that it has actually failed. If not, then set the status and current date.
		if (backupMetadata.getCompleted() == null)
			backupMetadata.setCompleted(Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTime());

		//Set this later to ensure the status
		if (backupMetadata.getStatus() != BackupMetadata.Status.FAILED)
			backupMetadata.setStatus(BackupMetadata.Status.FAILED);

		//Retrieve the snapshot metadata and then update the failure date/status.
		retrieveAndUpdate(backupMetadata);

		//Save the backupMetaDataMap
		save(backupMetadata);
	}

	public abstract void save(BackupMetadata backupMetadata);

	public abstract LinkedList<BackupMetadata> fetch(String snapshotDate);

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("BackupStatusMgr{");
		sb.append("backupMetadataMap=").append(backupMetadataMap);
		sb.append(", capacity=").append(capacity);
		sb.append('}');
		return sb.toString();
	}
}
