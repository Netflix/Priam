package com.netflix.priam.backup;

import java.util.Date;
import java.util.List;

/**
 * Created by aagrawal on 1/30/17.
 */
public interface IBackupStatusMgr {

    public void add(IMessageObserver.BACKUP_MESSAGE_TYPE message_type, String backup, Date startTime, Date completedTime);
    public BackupMetadata locate(IMessageObserver.BACKUP_MESSAGE_TYPE message_type, String date);
}
