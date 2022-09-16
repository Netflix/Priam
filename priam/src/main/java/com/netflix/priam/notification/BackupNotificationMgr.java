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
import com.google.common.base.Splitter;
import com.google.inject.Inject;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.BackupVerificationResult;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.identity.config.InstanceInfo;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A means to nofity interested party(ies) of an uploaded file, success or failed.
 *
 * <p>Created by vinhn on 10/30/16.
 */
public class BackupNotificationMgr implements EventObserver<BackupEvent> {

    private static final String SUCCESS_VAL = "success";
    private static final String FAILED_VAL = "failed";
    private static final String STARTED = "started";
    private static final Logger logger = LoggerFactory.getLogger(BackupNotificationMgr.class);
    private final IConfiguration config;
    private final IBackupRestoreConfig backupRestoreConfig;
    private final INotificationService notificationService;
    private final InstanceInfo instanceInfo;
    private final InstanceIdentity instanceIdentity;
    private final Set<AbstractBackupPath.BackupFileType> notifiedBackupFileTypesSet;
    private String notifiedBackupFileTypes;

    @Inject
    public BackupNotificationMgr(
            IConfiguration config,
            IBackupRestoreConfig backupRestoreConfig,
            INotificationService notificationService,
            InstanceInfo instanceInfo,
            InstanceIdentity instanceIdentity) {
        this.config = config;
        this.backupRestoreConfig = backupRestoreConfig;
        this.notificationService = notificationService;
        this.instanceInfo = instanceInfo;
        this.instanceIdentity = instanceIdentity;
        this.notifiedBackupFileTypesSet = new HashSet<>();
        this.notifiedBackupFileTypes = "";
    }

    public void notify(BackupVerificationResult backupVerificationResult) {
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("s3bucketname", this.config.getBackupPrefix());
            jsonObject.put("s3clustername", config.getAppName());
            jsonObject.put("s3namespace", backupVerificationResult.remotePath);
            jsonObject.put("region", instanceInfo.getRegion());
            jsonObject.put("rack", instanceInfo.getRac());
            jsonObject.put("token", instanceIdentity.getInstance().getToken());
            jsonObject.put("backuptype", "SNAPSHOT_VERIFIED");
            jsonObject.put("snapshotInstant", backupVerificationResult.snapshotInstant);
            // SNS Attributes for filtering messages. Cluster name and backup file type.
            Map<String, MessageAttributeValue> messageAttributes =
                    getMessageAttributes(
                            AbstractBackupPath.BackupFileType.SNAPSHOT_VERIFIED, jsonObject);

            this.notificationService.notify(jsonObject.toString(), messageAttributes);
        } catch (JSONException exception) {
            logger.error(
                    "JSON exception during generation of notification for snapshot verification: {}. Msg: {}",
                    backupVerificationResult,
                    exception.getLocalizedMessage());
        }
    }

    private Map<String, MessageAttributeValue> getMessageAttributes(
            AbstractBackupPath.BackupFileType backupFileType, JSONObject additionalAttributes) {
        // SNS Attributes for filtering messages. Cluster name and backup file type.
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.putIfAbsent(
                "s3clustername",
                new MessageAttributeValue()
                        .withDataType("String")
                        .withStringValue(config.getAppName()));
        messageAttributes.putIfAbsent(
                "backuptype",
                new MessageAttributeValue()
                        .withDataType("String")
                        .withStringValue(backupFileType.name()));

        String additionalAttrs =
                this.backupRestoreConfig.getBackupNotificationAdditionalMessageAttrs();
        if (!StringUtils.isBlank(additionalAttrs)) {
            try {
                List<String> attrs =
                        Splitter.on(",")
                                .omitEmptyStrings()
                                .trimResults()
                                .splitToList(additionalAttrs);
                for (String attr : attrs) {
                    Object value = additionalAttributes.get(attr);
                    if (value != null) {
                        // Does not handle all data types
                        if ("Instant".equals(value.getClass().getSimpleName())) {
                            byte[] byteValue = SerializationUtils.serialize((Instant) value);
                            messageAttributes.putIfAbsent(
                                    attr,
                                    new MessageAttributeValue()
                                            .withDataType("Binary")
                                            .withBinaryValue(ByteBuffer.wrap(byteValue)));
                        } else {
                            messageAttributes.putIfAbsent(
                                    attr,
                                    new MessageAttributeValue()
                                            .withDataType("String")
                                            .withStringValue(value.toString()));
                        }
                    }
                }
            } catch (Exception e) {
                logger.debug(
                        "Exception occurred while processing of additional message attributes", e);
            }
        }

        return messageAttributes;
    }

    private void notify(AbstractBackupPath abp, String uploadStatus) {
        JSONObject jsonObject = new JSONObject();
        try {
            Set<AbstractBackupPath.BackupFileType> updatedNotifiedBackupFileTypeSet =
                    getUpdatedNotifiedBackupFileTypesSet(this.notifiedBackupFileTypes);
            if (updatedNotifiedBackupFileTypeSet.isEmpty()
                    || updatedNotifiedBackupFileTypeSet.contains(abp.getType())) {
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
                Map<String, MessageAttributeValue> messageAttributes =
                        getMessageAttributes(abp.getType(), jsonObject);

                this.notificationService.notify(jsonObject.toString(), messageAttributes);
            } else {
                logger.debug(
                        "BackupFileType {} is not in the list of notified component types {}",
                        abp.getType().name(),
                        StringUtils.join(notifiedBackupFileTypesSet, ", "));
            }
        } catch (JSONException exception) {
            logger.error(
                    "JSON exception during generation of notification for upload {}.  Local file {}. Ignoring to continue with rest of backup.  Msg: {}",
                    uploadStatus,
                    abp.getFileName(),
                    exception.getLocalizedMessage());
        }
    }

    private Set<AbstractBackupPath.BackupFileType> getUpdatedNotifiedBackupFileTypesSet(
            String notifiedBackupFileTypes) {
        String propertyValue = this.backupRestoreConfig.getBackupNotifyComponentIncludeList();
        if (!notifiedBackupFileTypes.equals(propertyValue)) {
            logger.info(
                    String.format(
                            "Notified BackupFileTypes changed from %s to %s",
                            this.notifiedBackupFileTypes, propertyValue));
            this.notifiedBackupFileTypesSet.clear();
            this.notifiedBackupFileTypes =
                    this.backupRestoreConfig.getBackupNotifyComponentIncludeList();
            if (!StringUtils.isBlank(this.notifiedBackupFileTypes)) {
                for (String s : this.notifiedBackupFileTypes.split(",")) {
                    try {
                        AbstractBackupPath.BackupFileType backupFileType =
                                AbstractBackupPath.BackupFileType.fromString(s.trim());
                        notifiedBackupFileTypesSet.add(backupFileType);
                    } catch (BackupRestoreException ignored) {
                    }
                }
            }
        }
        return Collections.unmodifiableSet(this.notifiedBackupFileTypesSet);
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
