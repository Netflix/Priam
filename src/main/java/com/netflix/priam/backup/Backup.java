package com.netflix.priam.backup;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.scheduler.Task;

/**
 * Abstract Backup class for uploading files to backup location
 */
public abstract class Backup extends Task
{
    List<String> FILTER_KEYSPACE = Arrays.asList("OpsCenter");
    List<String> FILTER_COLUMN_FAMILY = Arrays.asList("LocationInfo");

    @Inject
    protected Provider<AbstractBackupPath> pathFactory;
    @Inject
    protected IBackupFileSystem fs;

    /**
     * Upload files in the specified dir
     * @param parent    Parent dir
     * @param type      Type of file (META, SST, SNAP etc)
     * @return
     * @throws ParseException
     * @throws BackupRestoreException
     * @throws IOException
     */
    public List<AbstractBackupPath> upload(File parent, BackupFileType type) throws ParseException, BackupRestoreException, IOException
    {
        List<AbstractBackupPath> bps = Lists.newArrayList();
        for (File file : parent.listFiles())
        {
            try
            {
                AbstractBackupPath bp = pathFactory.get();
                bp.parseLocal(file, type);
                String[] cfPrefix = bp.fileName.split("-");
                if (cfPrefix.length > 1 && FILTER_COLUMN_FAMILY.contains(cfPrefix[0]))
                    continue;
                fs.upload(bp);
                bps.add(bp);
            }
            finally
            {
                file.delete();
            }
        }
        return bps;
    }

    /**
     * Filters unwanted keyspaces and column families
     * @param keyspaceDir
     * @param backupDir
     * @return
     */
    public boolean isValidBackupDir(File keyspaceDir, File backupDir)
    {
        if (!backupDir.isDirectory() && !backupDir.exists())
            return false;
        String keyspaceName = keyspaceDir.getName();
        if (FILTER_KEYSPACE.contains(keyspaceName))
            return false;
        return true;
    }
}
