package com.netflix.priam.backup;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
	private int capacity;
	@Inject
	public BackupStatusMgr(IConfiguration config) {
		this.capacity = 60;  //TODO: fetch from properties
	}

	/*
	 * Will add or update (a new snapshot for the same key (day)).  The start / completed time applies to the most recent 
	 * snapshot.
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
			logger.info("Adding val to existing snapshot: " + key);
			
		} else {
			BackupMetadata c = new BackupMetadata(key, backup);
			c.setCompletedTime(completedTime);
			c.setStartTime(startTime);
			
			try {
				String token = SystemUtils.getDataFromUrl("http://localhost:8080/Priam/REST/v1/cassconfig/get_token");
				c.setToken(token);				
			} catch (Exception e) {
				logger.warn("Backup metadata will not have a token as I was not able to fetch it.");
			}

			
			bkups.add(c);
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
	
	public BackupMetadata locate(String key) {
		BackupMetadata res = null;
		//start with the most recent bakcup
		Iterator<BackupMetadata> descIt = bkups.descendingIterator();
		while (descIt.hasNext()) {
			BackupMetadata c = descIt.next();
			if (c.getKey().equals(key)) {
				res = c;
				logger.info("Found backup for " + key);
				break;
			}
		}
		
		return res;
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
	 * Encapsulates metadata for a backup.
	 */
	public static class BackupMetadata {
		private List<String> backups = new ArrayList<String>();
		private String key;
		private String token;
		private Date start, completed;

		public BackupMetadata(String key, String val) {
			this.key = key;
			backups.add(val);
		}

		/*
		 * @return a list of all backups for the day, empty list if no backups.
		 */
		public Collection<String> getBackups() {
			return backups;
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