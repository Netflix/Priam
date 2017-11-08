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
package com.netflix.priam.notification;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A means to nofity interested party(ies) of an uploaded file, success or failed.
 *
 * Created by vinhn on 10/30/16.
 */
public class BackupNotificationMgr implements EventObserver<BackupEvent> {

    public static final String SUCCESS_VAL = "success", FAILED_VAL = "failed", STARTED = "started";
    private static final Logger logger = LoggerFactory.getLogger(BackupNotificationMgr.class);
    private final IConfiguration config;
    private INotificationService notificationService;

    @Inject
    public BackupNotificationMgr(IConfiguration config, INotificationService notificationService) {
        this.config = config;
        this.notificationService = notificationService;
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
            jsonObject.put("rack", this.config.getRac());
            jsonObject.put("token", abp.getNodeIdentifier());
            jsonObject.put("nodeIdentifier", abp.getNodeIdentifier());
            jsonObject.put("filename", abp.getFileName());
            jsonObject.put("uncompressfilesize", abp.getSize());
            jsonObject.put("compressfilesize", abp.getCompressedFileSize());
            jsonObject.put("backuptype", abp.getType().name());
            jsonObject.put("uploadstatus", uploadStatus);
            this.notificationService.notify(jsonObject.toString());
        } catch (JSONException exception) {
            logger.error("JSON exception during generation of notification for upload {}.  Local file {}. Ignoring to continue with rest of backup.  Msg: {}", uploadStatus, abp.getFileName(), exception.getLocalizedMessage());
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
