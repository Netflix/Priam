package com.netflix.priam.backup;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.Task;
import com.netflix.priam.utils.RetryableCallable;

/**
 * Abstract Backup class for uploading files to backup location
 */
public abstract class AbstractBackup extends Task
{
    protected final List<String> FILTER_KEYSPACE = Arrays.asList("OpsCenter");
    protected final List<String> FILTER_COLUMN_FAMILY = Arrays.asList("LocationInfo");
    protected final Provider<AbstractBackupPath> pathFactory;
    protected final IBackupFileSystem fs;

    @Inject
    public AbstractBackup(IConfiguration config, IBackupFileSystem fs, Provider<AbstractBackupPath> pathFactory)
    {
        super(config);
        this.fs = fs;
        this.pathFactory = pathFactory;
    }

    /**
     * Upload files in the specified dir. Does not delete the file in case of
     * error
     * 
     * @param parent
     *            Parent dir
     * @param type
     *            Type of file (META, SST, SNAP etc)
     * @return
     * @throws Exception
     */
    protected List<AbstractBackupPath> upload(File parent, BackupFileType type) throws Exception
    {
        List<AbstractBackupPath> bps = Lists.newArrayList();
        for (File file : parent.listFiles())
        {
            final AbstractBackupPath bp = pathFactory.get();
            bp.parseLocal(file, type);
            upload(bp);
            bps.add(bp);
            file.delete();
        }
        return bps;
    }

    /**
     * Upload specified file (RandomAccessFile) with retries
     */
    protected void upload(final AbstractBackupPath bp) throws Exception
    {
        new RetryableCallable<Void>()
        {
            @Override
            public Void retriableCall() throws Exception
            {
                fs.upload(bp, bp.localReader());
                return null;
            }
        }.call();
    }

    /**
     * Filters unwanted keyspaces and column families
     */
    public boolean isValidBackupDir(File keyspaceDir, File columnFamilyDir, File backupDir)
    {
        if (!backupDir.isDirectory() && !backupDir.exists())
            return false;
        String keyspaceName = keyspaceDir.getName();
        if (FILTER_KEYSPACE.contains(keyspaceName))
            return false;
        String columnFamilyName = columnFamilyDir.getName();
        if (FILTER_COLUMN_FAMILY.contains(columnFamilyName))
            return false;
        return true;
    }
}
