/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.priam.backup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.notification.BackupEvent;
import com.netflix.priam.notification.BackupNotificationMgr;
import com.netflix.priam.notification.EventGenerator;
import com.netflix.priam.notification.EventObserver;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.RetryableCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract Backup class for uploading files to backup location
 */
public abstract class AbstractBackup extends Task implements EventGenerator<BackupEvent> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractBackup.class);

    public static String JOBNAME = "AbstractBackup";

    protected final List<String> FILTER_KEYSPACE = Arrays.asList("OpsCenter");
    protected final Map<String, List<String>> FILTER_COLUMN_FAMILY = ImmutableMap.of("system", Arrays.asList("local", "peers", "LocationInfo"));
    protected final Provider<AbstractBackupPath> pathFactory;
    protected final Map<String, List<String>> columnFamilyFilter = new HashMap<>(); //key: keyspace, value: a list of CFs within the keyspace
    protected final Map<String, Object> keyspaceFilter = new HashMap<>(); //key: keyspace, value: null
    private final Object MUTEX = new Object();
    protected IBackupFileSystem fs;
    private List<EventObserver> observers = new ArrayList<>();

    @Inject
    public AbstractBackup(IConfiguration config, @Named("backup") IFileSystemContext backupFileSystemCtx,
                          Provider<AbstractBackupPath> pathFactory,
                          BackupNotificationMgr backupNotificationMgr) {
        super(config);
        this.pathFactory = pathFactory;
        this.fs = backupFileSystemCtx.getFileStrategy(config);
        this.addObserver(backupNotificationMgr);
    }

    /*
     * A means to override the type of backup strategy chosen via BackupFileSystemContext
     */
    protected void setFileSystem(IBackupFileSystem fs) {
        this.fs = fs;
    }

    /*
     * search for "1:* alphanumeric chars including special chars""literal period"" 1:* alphanumeric chars  including special chars"
     * @param input string
     * @return true if input string matches search pattern; otherwise, false
     */
    protected boolean isValidCFFilterFormat(String cfFilter) {
        Pattern p = Pattern.compile(".\\..");
        Matcher m = p.matcher(cfFilter);
        return m.find();
    }

    /**
     * Upload files in the specified dir. Does not delete the file in case of
     * error.  The files are uploaded serially.
     *
     * @param parent Parent dir
     * @param type   Type of file (META, SST, SNAP etc)
     * @return List of files that are successfully uploaded as part of backup
     * @throws Exception when there is failure in uploading files.
     */
    protected List<AbstractBackupPath> upload(File parent, final BackupFileType type) throws Exception {
        final List<AbstractBackupPath> bps = Lists.newArrayList();
        for (final File file : parent.listFiles()) {
            //== decorate file with metadata
            final AbstractBackupPath bp = pathFactory.get();
            bp.parseLocal(file, type);

            try {
                logger.info(String.format("About to upload file %s for backup", file.getCanonicalFile()));

                AbstractBackupPath abp = new RetryableCallable<AbstractBackupPath>(3, RetryableCallable.DEFAULT_WAIT_TIME) {
                    public AbstractBackupPath retriableCall() throws Exception {
                        upload(bp);
                        file.delete();
                        return bp;
                    }
                }.call();

                if (abp != null)
                    bps.add(abp);

                addToRemotePath(abp.getRemotePath());
            } catch (Exception e) {
                logger.error(String.format("Failed to upload local file %s within CF %s. Ignoring to continue with rest of backup.", file.getCanonicalFile(), parent.getAbsolutePath()), e);
            }
        }
        return bps;
    }


    /**
     * Upload specified file (RandomAccessFile) with retries
     *
     * @param bp backup path to be uplaoded.
     */
    protected void upload(final AbstractBackupPath bp) throws Exception {
        new RetryableCallable<Void>() {
            @Override
            public Void retriableCall() throws Exception {
                java.io.InputStream is = null;
                try {
                    is = bp.localReader();
                    if (is == null) {
                        throw new NullPointerException("Unable to get handle on file: " + bp.fileName);
                    }
                    fs.upload(bp, is);
                    bp.setCompressedFileSize(fs.getBytesUploaded());
                    bp.setAWSSlowDownExceptionCounter(fs.getAWSSlowDownExceptionCounter());
                    return null;
                } catch (Exception e) {
                    logger.error(String.format("Exception uploading local file %S,  releasing handle, and will retry."
                            , bp.backupFile.getCanonicalFile()));
                    if (is != null) {
                        is.close();
                    }
                    throw e;
                }

            }
        }.call();
    }

    /**
     * Filters unwanted keyspaces and column families
     */
    public boolean isValidBackupDir(File keyspaceDir, File columnFamilyDir, File backupDir) {
        if (!backupDir.isDirectory() && !backupDir.exists())
            return false;
        String keyspaceName = keyspaceDir.getName();
        if (FILTER_KEYSPACE.contains(keyspaceName)) {
            logger.debug("{} is not consider a valid keyspace backup directory, will be bypass.", keyspaceName);
            return false;
        }

        String dirName = columnFamilyDir.getName();

        String columnFamilyName = dirName.split("-")[0];
        if (FILTER_COLUMN_FAMILY.containsKey(keyspaceName) && FILTER_COLUMN_FAMILY.get(keyspaceName).contains(columnFamilyName)) {
            logger.debug("{} is not consider a valid CF backup directory, will be bypass.", dirName);
            return false;
        }

        return true;
    }

    /**
     * Adds Remote path to the list of Remote Paths
     */
    protected abstract void addToRemotePath(String remotePath);

    @Override
    public void addObserver(EventObserver observer) {
        if (observers == null)
            observers = new ArrayList<>();

        synchronized (MUTEX) {
            if (!observers.contains(observer))
                observers.add(observer);
        }
    }

    @Override
    public void removeObserver(EventObserver observer) {
        if (observers == null || observers.isEmpty())
            return;
        synchronized (MUTEX) {
            observers.remove(observer);
        }
    }

    @Override
    public void notifyEventStart(BackupEvent event) {
        if (shouldNotifyObservers())
            observers.forEach(eventObserver -> eventObserver.updateEventStart(event));
    }

    @Override
    public void notifyEventSuccess(BackupEvent event) {
        if (shouldNotifyObservers())
            observers.forEach(eventObserver -> eventObserver.updateEventSuccess(event));
    }

    @Override
    public void notifyEventFailure(BackupEvent event) {
        if (shouldNotifyObservers())
            observers.forEach(eventObserver -> eventObserver.updateEventFailure(event));
    }

    @Override
    public void notifyEventStop(BackupEvent event) {
        if (shouldNotifyObservers())
            observers.forEach(eventObserver -> eventObserver.updateEventStop(event));
    }

    private boolean shouldNotifyObservers() {
        return (observers != null && !observers.isEmpty());
    }

    /*
    * @param keyspace or columnfamily directory type.
    * @return true if directory should be filter from processing; otherwise, false.
    */
    protected boolean isFiltered(DIRECTORYTYPE directoryType, String... args) {

        if (directoryType.equals(DIRECTORYTYPE.KEYSPACE)) { //start with filtering the parent (keyspace)
            //Apply each keyspace filter to input string
            String keyspaceName = args[0];

            java.util.Set<String> ksFilters = keyspaceFilter.keySet();
            Iterator<String> it = ksFilters.iterator();
            while (it.hasNext()) {
                String ksFilter = it.next();
                Pattern p = Pattern.compile(ksFilter);
                Matcher m = p.matcher(keyspaceName);
                if (m.find()) {
                    logger.info("Keyspace: " + keyspaceName + " matched filter: " + ksFilter);
                    return true;
                }
            }

        }

        if (directoryType.equals(DIRECTORYTYPE.CF)) { //parent (keyspace) is not filtered, now see if the child (CF) is filtered
            String keyspaceName = args[0];
            if (!columnFamilyFilter.containsKey(keyspaceName)) {
                return false;
            }

            String cfName = args[1];
            List<String> cfsFilter = columnFamilyFilter.get(keyspaceName);
            for (int i = 0; i < cfsFilter.size(); i++) {
                Pattern p = Pattern.compile(cfsFilter.get(i));
                Matcher m = p.matcher(cfName);
                if (m.find()) {
                    logger.info(keyspaceName + "." + cfName + " matched filter");
                    return true;
                }
            }
        }

        return false; //if here, current input are not part of keyspae and cf filters
    }

    protected void populateFilters() {
        String configKeyspaceFilter = getConfigKeyspaceFilter();
        if (configKeyspaceFilter == null || configKeyspaceFilter.isEmpty()) {
            logger.info(String.format("No keyspace filter set for {}.", JOBNAME));
        } else {
            String[] keyspaces = configKeyspaceFilter.split(",");
            for (int i = 0; i < keyspaces.length; i++) {
                logger.info(String.format("Adding {} keyspace filter: {}", JOBNAME, keyspaces[i]));
                this.keyspaceFilter.put(keyspaces[i], null);
            }

        }

        String configColumnfamilyFilter = getConfigColumnfamilyFilter();
        if (configColumnfamilyFilter == null || configColumnfamilyFilter.isEmpty()) {

            logger.info(String.format("No column family filter set for {}.", JOBNAME));

        } else {

            String[] filters = configColumnfamilyFilter.split(",");
            for (int i = 0; i < filters.length; i++) { //process each filter
                if (isValidCFFilterFormat(filters[i])) {

                    String[] filter = filters[i].split("\\.");
                    String ksName = filter[0];
                    String cfName = filter[1];
                    logger.info(String.format("Adding {} CF filter: {}.{}", JOBNAME, ksName, cfName));

                    if (this.columnFamilyFilter.containsKey(ksName)) {
                        //add cf to existing filter
                        List<String> cfs = this.columnFamilyFilter.get(ksName);
                        cfs.add(cfName);
                        this.columnFamilyFilter.put(ksName, cfs);

                    } else {

                        List<String> cfs = new ArrayList<String>();
                        cfs.add(cfName);
                        this.columnFamilyFilter.put(ksName, cfs);

                    }

                } else {
                    throw new IllegalArgumentException("Column family filter format is not valid.  Format needs to be \"keyspace.columnfamily\".  Invalid input: " + filters[i]);
                }
            } //end processing each filter

        }
    }

    protected abstract String getConfigKeyspaceFilter();

    protected abstract String getConfigColumnfamilyFilter();

    public enum DIRECTORYTYPE {
        KEYSPACE, CF
    }

}