/**
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.backup;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.utils.RetryableCallable;


//Providing this if we want to use it outside Quart
public class CommitLogBackup
{
  private static final Logger logger = LoggerFactory.getLogger(CommitLogBackup.class);
  private final Provider<AbstractBackupPath> pathFactory;
  static List<IMessageObserver> observers = new ArrayList();
  private final List<String> clRemotePaths = new ArrayList();
  private final IBackupFileSystem fs;

  @Inject
  public CommitLogBackup(Provider<AbstractBackupPath> pathFactory, @Named("backup") IBackupFileSystem fs)
  {
    this.pathFactory = pathFactory;
    this.fs = fs;
  }

  public List<AbstractBackupPath> upload(String archivedDir, final String snapshotName)
    throws Exception
  {
    logger.info("Inside upload CommitLog files");

    if (StringUtils.isBlank(archivedDir))
    {
    	throw new IllegalArgumentException("The archived commitlog director is blank or null");
    }
    
    File archivedCommitLogDir = new File(archivedDir);
    if (!archivedCommitLogDir.exists())
    {
       throw new IllegalArgumentException("The archived commitlog director does not exist: " + archivedDir);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Scanning for backup in: {}", archivedCommitLogDir.getAbsolutePath());
    }
    List bps = Lists.newArrayList();
    for (final File file : archivedCommitLogDir.listFiles())
    {
      logger.debug(String.format("Uploading commit log %s for backup", new Object[] { file.getCanonicalFile() }));
      try
      {
        AbstractBackupPath abp = (AbstractBackupPath)new RetryableCallable(3, 100L)
        {
          public AbstractBackupPath retriableCall() throws Exception
          {

            AbstractBackupPath bp = (AbstractBackupPath) pathFactory.get();
            bp.parseLocal(file, BackupFileType.CL);
            if (snapshotName != null)
            	bp.time = bp.parseDate(snapshotName);
            upload(bp);
            file.delete(); //TODO: should we put delete call here? We don't want to delete if the upload operaion fails
            return bp;
          }
        }
        .call();

        if (abp != null) {
          bps.add(abp);
        }
        addToRemotePath(abp.getRemotePath());
      }
      catch (Exception e)
      {
        logger.error(String.format("Failed to upload local file %s. Ignoring to continue with rest of backup.", new Object[] { file }), e);
      }
    }
    return bps;
  }

  private void upload(final AbstractBackupPath bp)
    throws Exception
  {
    new RetryableCallable()
    {
      public Void retriableCall()
        throws Exception
      {
        fs.upload(bp, bp.localReader());
        return null;
      }
    }
    .call();
  }

  public static void addObserver(IMessageObserver observer)
  {
    observers.add(observer);
  }

  public static void removeObserver(IMessageObserver observer) {
    observers.remove(observer);
  }

  public void notifyObservers() {
    for (IMessageObserver observer : observers)
      if (observer != null) {
        logger.debug("Updating CommitLog observers now ...");
        observer.update(IMessageObserver.BACKUP_MESSAGE_TYPE.COMMITLOG, this.clRemotePaths);
      } else {
        logger.debug("Observer is Null, hence can not notify ...");
      }
  }

  protected void addToRemotePath(String remotePath) {
    this.clRemotePaths.add(remotePath);
  }
}