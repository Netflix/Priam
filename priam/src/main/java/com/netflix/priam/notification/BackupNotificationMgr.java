package com.netflix.priam.notification;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * A means to nofity interested party(ies) of an uploaded file, success or failed.
 *
 * Created by vinhn on 10/30/16.
 */
public class BackupNotificationMgr {

    public static final String SUCCESS_VAL = "success", FAILED_VAL = "failed";

    private final IConfiguration config;
    private INotificationService notificationService;

    @Inject
    public BackupNotificationMgr(IConfiguration config, @Named("defaultnotificationservice") INotificationService notificationService) {
        this.config = config;
        this.notificationService = notificationService;
    }

    public void notify(AbstractBackupPath abp, String uploadStatus) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("s3bucketname", this.config.getBackupPrefix());
        jsonObject.put("s3clustername", abp.getClusterName());
        jsonObject.put("s3namespace", abp.getRemotePath());
        jsonObject.put("keyspace", abp.getKeyspace());
        jsonObject.put("cf", abp.getColumnFamily());
        jsonObject.put("region", abp.getRegion());
        jsonObject.put("rack", this.config.getRac());
        jsonObject.put("token", abp.getToken());
        jsonObject.put("filename", abp.getFileName());
        jsonObject.put("filesize", abp.getSize());
        jsonObject.put("backuptype", abp.getType().name());
        jsonObject.put("uploadstatus", uploadStatus);

        this.notificationService.notifiy(jsonObject.toString());
    }

    public static class NotificationMessage {
        private String backupType;  //name of enum AbstractBackupPath.BackupFileType
        private String s3BucketName, keyspace, CF, DC, token, uploadStatus;
        private String rack;  //zone

        public String getBackupType() {
            return backupType;
        }
        public void setBackupType(String backupType) {
            this.backupType = backupType;
        }
        public String getKeyspace() {
            return keyspace;
        }
        public void setKeyspace(String keyspace) {
            this.keyspace = keyspace;
        }
        public String getUploadStatus() {
            return uploadStatus;
        }
        public void setUploadStatus(String uploadStatus) {
            this.uploadStatus = uploadStatus;
        }
        public String getToken() {
            return token;
        }
        public void setToken(String token) {
            this.token = token;
        }
        public String getDC() {
            return DC;
        }
        public void setDC(String dC) {
            DC = dC;
        }
        public String getCF() {
            return CF;
        }
        public void setCF(String cF) {
            CF = cF;
        }
        public String getS3BucketName() {
            return s3BucketName;
        }
        public void setS3BucketName(String s3BucketName) {
            this.s3BucketName = s3BucketName;
        }
        public String getRack() {
            return rack;
        }
        public void setRack(String rack) {
            this.rack = rack;
        }
    }
}
