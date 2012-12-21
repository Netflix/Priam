package com.netflix.priam.backup;

import java.util.List;


public interface IMessageObserver {

	public enum BACKUP_MESSAGE_TYPE {SNAPSHOT, INCREMENTAL, COMMITLOG};
	public void update(BACKUP_MESSAGE_TYPE bkpMsgType, List<String> remotePathNames);

}
