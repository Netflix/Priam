/*
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.netflix.priam.backup;

import java.util.List;

public interface IMessageObserver {

    enum BACKUP_MESSAGE_TYPE {
        SNAPSHOT,
        INCREMENTAL,
        COMMITLOG,
        META
    }

    enum RESTORE_MESSAGE_TYPE {
        SNAPSHOT,
        INCREMENTAL,
        COMMITLOG,
        META
    }

    enum RESTORE_MESSAGE_STATUS {
        UPLOADED,
        DOWNLOADED,
        STREAMED
    }

    void update(BACKUP_MESSAGE_TYPE bkpMsgType, List<String> remotePathNames);

    void update(
            RESTORE_MESSAGE_TYPE rstMsgType,
            List<String> remotePathNames,
            RESTORE_MESSAGE_STATUS rstMsgStatus);

    void update(
            RESTORE_MESSAGE_TYPE rstMsgType,
            String remotePath,
            String fileDiskPath,
            RESTORE_MESSAGE_STATUS rstMsgStatus);
}
