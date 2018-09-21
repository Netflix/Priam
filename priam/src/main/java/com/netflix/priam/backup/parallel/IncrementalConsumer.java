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
package com.netflix.priam.backup.parallel;

import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.utils.RetryableCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

/*
 * Performs an upload of a file, with retries.
 */
public class IncrementalConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(IncrementalConsumer.class);

    private AbstractBackupPath bp;
    private IBackupFileSystem fs;
    private BackupPostProcessingCallback<AbstractBackupPath> callback;

    /**
     * Upload files. Does not delete the file in case of
     * error.
     *
     * @param bp - the file to upload with additional metada
     */
    public IncrementalConsumer(AbstractBackupPath bp, IBackupFileSystem fs
            , BackupPostProcessingCallback<AbstractBackupPath> callback
    ) {
        this.bp = bp;
        this.bp.setType(AbstractBackupPath.BackupFileType.SST);  //Tag this is an incremental upload, not snapshot
        this.fs = fs;
        this.callback = callback;
    }

    @Override
    /*
     * Upload specified file, with retries logic.
	 * File will be deleted only if uploaded successfully.
	 */
    public void run() {

        logger.info("Consumer - about to upload file: {}", this.bp.getFileName());

        try {
                    if (!bp.getBackupFile().exists())
                        throw new java.util.concurrent.CancellationException("Someone beat me to uploading this file"
                                + ", no need to retry.  Most likely not needed but to be safe, checked and released handle to file if appropriate.");

            fs.uploadFile(Paths.get(bp.getBackupFile().getAbsolutePath()), Paths.get(bp.getRemotePath()), bp, 10, true);
            this.callback.postProcessing(bp); //post processing
        } catch (Exception e) {
                logger.error("Failed to upload local file {}. Ignoring to continue with rest of backup.  Msg: {}", this.bp.getFileName(), e.getLocalizedMessage());
            }
        } finally {
            // post processing must happen regardless of the outcome of the upload. Otherwise we can
            // leak tasks into the underlying taskMgr queue and prevent re-enqueues of the upload itself
            this.callback.postProcessing(bp);
        }
        logger.info("Consumer - done with upload file: {}", this.bp.getFileName());
    }

}
