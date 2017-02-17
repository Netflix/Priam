package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.SystemUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;

/*
 * A means to manage metadata for various types of backups (snapshots, incrementals)
 */
@Singleton
public class BackupStatusMgr implements IBackupStatusMgr{

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
	public void add(IMessageObserver.BACKUP_MESSAGE_TYPE message_type, String backup, Date startTime, Date completedTime) {
		String key = formatKey(message_type, startTime);
		if (bkups.size() == this.capacity) {
			bkups.removeFirst(); //Remove the oldest backup
		}

		BackupMetadata b = locate(message_type, new DateTime(startTime).toString("yyyyMMdd"));
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
	Will locate the backup for a type (incremental or snapshot) for a day (yyyymmdd)
	status first in memory, if not found, look on disk.
	@param key See this.formatkey(...) for expected format
	*/
	public BackupMetadata locate(IMessageObserver.BACKUP_MESSAGE_TYPE message_type, String date) {
		//start with the most recent bakcup
        String key = formatKey(message_type, date);
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

		BackupMetadata result = null;
		try {
			BufferedReader br = SystemUtils.readFile(this.config.getBackupStatusFileLoc());
			String raw = null;
			while((raw = br.readLine()) != null) {
				logger.info("Found backup status in data store.  Raw string: " + raw);
				BackupMetadata metadata = unmarshall(raw);
				bkups.add(metadata);
				if (metadata.getKey().equals(key))
					result = metadata;
			}
		} catch(FileNotFoundException fnfe)
		{
			logger.warn("Backup status file does not exist.", fnfe);
		} catch (IOException e) {
			logger.warn("Backup status file (" + key + ") exist in data store but unable to read file.  Msg: " + e.getLocalizedMessage());
		}

		return result;
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
		result.getBackups().add(SystemUtils.formatDate(result.getStartTime(),"yyyyMMddHHmm"));
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

}
