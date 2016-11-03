package com.netflix.priam.backup;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.SystemUtils;

/*
 * A means to manage metadata for various types of backups (snapshots, incrementals)
 */
@Singleton
public class BackupStatusMgr {

	private static final Logger logger = LoggerFactory.getLogger(BackupStatusMgr.class);
	
	 /*
	  * completed bkups represented by its snapshot name (yyyymmddhhmm)
	  * Note:  A linkedlist was chosen for fastest removal of the oldest backup.
	  */
	private final LinkedList<BackupMetadata> bkups = new LinkedList<BackupMetadata>();
	private final IConfiguration config;
	private int capacity;
	@Inject
	public BackupStatusMgr(IConfiguration config) {
		this.config = config;
		this.capacity = 60;  //TODO: fetch from properties
	}

	/*
	 * Will add or update (a new snapshot for the same key (day)).  The start / completed time applies to the most recent 
	 * snapshot.
	 * @param key see this.formatkey(...) for expected format (e.g. SNAPSHOT_20161102)
	 * @para backup snapshot name format of yyyymmddhhss
	 * @param startime of backup
	 * @param completion time of backup
	 */
	public BackupMetadata add(String key, String backup, Date startTime, Date completedTime) {
		if (bkups.size() == this.capacity) {
			bkups.removeFirst(); //Remove the oldest backup
		}
		BackupMetadata b = locate(key);
		if (b != null) {
			b.getBackups().add(backup);
			b.setStartTime(startTime);
			b.setCompletedTime(completedTime);

			String str = marshall(b, backup);
			SystemUtils.writeToFile(this.config.getBackupStatusFileLoc(), str);

			logger.info("Adding val to existing snapshot: " + key);
			
		} else {
			BackupMetadata c = new BackupMetadata(key, backup);
			c.setCompletedTime(completedTime);
			c.setStartTime(startTime);
			
			String token = SystemUtils.getDataFromUrl("http://localhost:8080/Priam/REST/v1/cassconfig/get_token");
			c.setToken(token);
			
			bkups.add(c);

			String str = marshall(c, backup);
			SystemUtils.writeToFile(this.config.getBackupStatusFileLoc(), str);

			logger.info("Adding new snapshot meta data: " + key);
		}
		
		return b;

	}

	public int capacity() {
		return this.capacity;
	}
	
	/*
	 * Generation of a key understanble to the backup status mgr.
	 * 
	 * @param backup type (e.g. SNAPSHOT, INCREENTAL)
	 * @param date of the backup
	 * @return key, format is backuptype_yyyymmdd
	 */
	public static String formatKey(IMessageObserver.BACKUP_MESSAGE_TYPE bkupType, Date date) {
		final String FMT = "yyyyMMdd";
		String s = new DateTime(date).toString(FMT);
		return bkupType.name() + "_" + s;
	}
	
	/*
	 * Generation of a key understanble to the backup status mgr.
	 * 
	 * @param backup type (e.g. SNAPSHOT, INCREENTAL)
	 * @param date of the backup, expected format is yyyymmdd
	 * @return key, format is backuptype_yyyymmdd
	 */
	public static String formatKey(IMessageObserver.BACKUP_MESSAGE_TYPE bkupType, String date) {
		return bkupType.name() + "_" + date;
	}

	/*
	 * Worse case, seek time is O(n).  The average seek time is O(1) as we expect majority of requests will be for the most
	 * recent backup date.
	 * 
	 * @param key.  See this.formatkey(...) for expected format
	 * 
	 */
	public Boolean status(String key) {
		Boolean result = false;
		if (locate(key) != null) {
			return true;
		} else {
			return false;
		}
		
	}

	/*
	Will locate the backup for a type (incremental or snapshot) for a day (yyyymmdd)
	status first in memory, if not found, look on disk.
	@param key See this.formatkey(...) for expected format
	*/
	public BackupMetadata locate(String key) {
		//start with the most recent bakcup
		Iterator<BackupMetadata> descIt = bkups.descendingIterator();
		while (descIt.hasNext()) {
			BackupMetadata c = descIt.next();
			if (c.getKey().equals(key)) {
				logger.debug("Found backup for " + key + " within cache");
				return c;
			}
		}

		//if here, key is not in cache, lets check data store
		BackupMetadata d = getBkupStatusFromDataStore(key);
		if (d == null || d.getKey() == null || d.getKey().isEmpty() ) {
			logger.warn("Backup (" + key + ") not found in data store.");
			return null;
		}

		if (d.getKey().equals(key)) {
			logger.info("Found backup: " + key + " within data store.");
			return d;
		} else {
			logger.warn("Backup (" + key + ") not found in data store.");
			return null;
		}
	}

	/*
	@param key See this.formatkey(...) for expected format.
	@return representation of backup for the day.  If not present, returns null.
 	*/
	private BackupMetadata getBkupStatusFromDataStore(String key) {
		BufferedReader br = null;
		try {
			br = SystemUtils.readFile(this.config.getBackupStatusFileLoc());
		} catch (IOException e) {
			logger.warn("Backup status file (" + key + ") does not exist in data store.  Msg: " + e.getLocalizedMessage());
			return null;
		}

		String raw = null;
		try {
			raw = br.readLine();
		} catch (IOException e) {
			logger.warn("Backup status file (" + key + ") exist in data store but unable to read file.  Msg: " + e.getLocalizedMessage());
			return null;
		}
		if (raw == null || raw.isEmpty() ) {
			logger.warn("Backup status file (" + key + ") exist in data store but is empty.");
			return null;
		}

		logger.info("Found backup status in data store.  Raw string: " + raw);
		return unmarshall(raw);
	}

	/*
	Transform primitive string to complex object.  E.g.
	SNAPSHOT_20161102=201611022010,starttime=Wed Nov 02 20:10:29 GMT 2016,completiontime=Wed Nov 02 20:12:43 GMT 2016,token=1808575600
	*/
	private BackupMetadata unmarshall(String raw) {
		String s[] = raw.split(",");
		if (s == null || s.length == 0) {
			return null;
		}

		String key = null, startTime = null, completionTime = null, token = null;
		String keyData = s[0];
		String keyNameValuePair[] = keyData.split("=");
		key = keyNameValuePair[0];

		for (int i=0; i < s.length; i++ ) {
			String t = s[i];
			String nvPairs[] = t.split("=");
			String name = nvPairs[0];
			String value = nvPairs[1];
			if (name.equalsIgnoreCase("starttime")) {
				startTime = value;
				continue;
			}
			if (name.equalsIgnoreCase("completiontime")) {
				completionTime = value;
				continue;
			}
			if (name.equalsIgnoreCase("token")) {
				token = value;
				continue;
			}
		}

		if (key == null || key.isEmpty()) {
			throw new IllegalStateException("Backup status file line is invalid misisng key.  Line: " + raw);
		}
		if (startTime == null || startTime.isEmpty() ) {
			throw new IllegalStateException("Backup status file line is invalid misisng start time.  Line: " + raw);
		}
		if (completionTime == null || completionTime.isEmpty() ) {
			throw new IllegalStateException("Backup status file line is invalid misisng completion time.  Line: " + raw);
		}
		if (token == null || token.isEmpty())  {
			throw new IllegalStateException("Backup status file line is invalid misisng token.  Line: " + raw);
		}

		BackupMetadata result = new BackupMetadata(key);
		result.setCompletedTime(new Date(completionTime));
		result.setToken(token);
		result.setStartTime(new Date(startTime));
		return result;
	}

	/*
    Transform complex object to primitive string.  Format of:
            [incremental\snapshot]_yyyymmdd=[success|failure],
    @param backup format of yyyymmddhhss
     */
	private String marshall(BackupMetadata b, String backup) {
		StringBuffer buffer = new StringBuffer();
		buffer.append(b.getKey());
		buffer.append('=');
		buffer.append(backup);
		buffer.append(',');
		buffer.append("starttime");
		buffer.append('=');
		buffer.append(b.getStartTime());
		buffer.append(',');
		buffer.append("completiontime");
		buffer.append('=');
		buffer.append(b.getCompletedTime());
		buffer.append(',');
		buffer.append("token");
		buffer.append('=');
		buffer.append(b.getToken());
		return buffer.toString();
	}


	/*
	 * @param the date of backups
	 * @return dates of backups for the specified date, if none, returns empty list.
	 * *Note: date. See this.formatkey(...) for expected format
	 * 
	 */
	public List<String> getBackups(String key) {
		List<String> result = new ArrayList<String>();
		BackupMetadata b = locate(key);
		if (b != null) {
			result.addAll(b.getBackups());
		}

		return result;
	}
	
	/*
	 * Encapsulates metadata for a backup for a day.
	 */
	public static class BackupMetadata {
		private List<String> backups = new ArrayList<String>();
		private String key;
		private String token;
		private Date start, completed;

		/*
		Represents a granular (includes hour and secs) backup for the day.
		@param key see formatKey() for format
		@param backupDate format is yyyymmddhhss
		 */
		public BackupMetadata(String key, String backupDate) {
			this.key = key;
			backups.add(backupDate);
		}
		/*
		Represents a high level(does not includ hour and secs) backup for the day.
		@param key see formatKey() for format
		@param backupDate format is yyyymmddhhss
 		*/
		public BackupMetadata(String key) {
			this.key = key;
		}

		/*
		 * @return a list of all backups for the day, empty list if no backups.
		 */
		public Collection<String> getBackups() {
			return backups;
		}

		public void setKey(String key) {
			this.key = key;
		}
		/*
		 * @return the date of the backup.  Format of date is yyyymmdd.
		 */
		public String getKey() {
			return this.key;
		}

		public String getToken() {
			return token;
		}

		public void setToken(String token) {
			this.token = token;
		}

		public Date getStartTime() {
			return start;
		}

		public void setStartTime(Date start) {
			this.start = start;
		}
		
		public void setCompletedTime(Date completed) {
			this.completed = completed;
		}
		
		public Date getCompletedTime() {
			return this.completed;
		}
	}
	
}
