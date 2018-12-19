/**
 * Copyright 2017 Netflix, Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.notification;

import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.google.inject.Inject;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.config.InstanceInfo;
import java.util.HashMap;
import java.util.Map;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A means to notify interested party(ies) of an uploaded file, success or failed.
 *
 * <p>Created by vinhn on 10/30/16.
 */
public class BackupNotificationMgr implements EventObserver<BackupEvent> {

    public static final String SUCCESS_VAL = "success", FAILED_VAL = "failed", STARTED = "started";
    private static final Logger logger = LoggerFactory.getLogger(BackupNotificationMgr.class);
    private final IConfiguration config;
    private final INotificationService notificationService;
    private final InstanceInfo instanceInfo;

    @Inject
    public BackupNotificationMgr(
            IConfiguration config,
            INotificationService notificationService,
            InstanceInfo instanceInfo) {
        this.config = config;
        this.notificationService = notificationService;
        this.instanceInfo = instanceInfo;
    }

    private void notify(AbstractBackupPath abp, String uploadStatus) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("s3bucketname", this.config.getBackupPrefix());
            jsonObject.put("s3clustername", abp.getClusterName());
            jsonObject.put("s3namespace", abp.getRemotePath());
            jsonObject.put("keyspace", abp.getKeyspace());
            jsonObject.put("cf", abp.getColumnFamily());
            jsonObject.put("region", abp.getRegion());
            jsonObject.put("rack", instanceInfo.getRac());
            jsonObject.put("token", abp.getToken());
            jsonObject.put("filename", abp.getFileName());
            jsonObject.put("uncompressfilesize", abp.getSize());
            jsonObject.put("compressfilesize", abp.getCompressedFileSize());
            jsonObject.put("backuptype", abp.getType().name());
            jsonObject.put("uploadstatus", uploadStatus);
            jsonObject.put("compression", abp.getCompression().name());
            jsonObject.put("encryption", abp.getEncryption().name());

            // SNS Attributes for filtering messages. Cluster name and backup file type.
            Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
            messageAttributes.putIfAbsent(
                    "s3clustername",
                    new MessageAttributeValue()
                            .withDataType("String")
                            .withStringValue(abp.getClusterName()));
            messageAttributes.putIfAbsent(
                    "backuptype",
                    new MessageAttributeValue()
                            .withDataType("String")
                            .withStringValue(abp.getType().name()));

            this.notificationService.notify(jsonObject.toString(), messageAttributes);
        } catch (JSONException exception) {
            logger.error(
                    "JSON exception during generation of notification for upload {}.  Local file {}. Ignoring to continue with rest of backup.  Msg: {}",
                    uploadStatus,
                    abp.getFileName(),
                    exception.getLocalizedMessage());
        }
    }

    @Override
    public void updateEventStart(BackupEvent event) {
        notify(event.getAbstractBackupPath(), STARTED);
    }

    @Override
    public void updateEventFailure(BackupEvent event) {
        notify(event.getAbstractBackupPath(), FAILED_VAL);
    }

    @Override
    public void updateEventSuccess(BackupEvent event) {
        notify(event.getAbstractBackupPath(), SUCCESS_VAL);
    }

    @Override
    public void updateEventStop(BackupEvent event) {
        // Do nothing.
    }
}
