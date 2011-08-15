package com.priam.backup;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.priam.backup.AbstractBackupPath.BackupFileType;
import com.priam.scheduler.Task;

public abstract class Backup extends Task
{
    // TODO make it a configuration (static one not dynamic which needs a
    // build).
    List<String> FILTER_KEYSPACE = Arrays.asList("OpsCenter");
    List<String> FILTER_COLUMN_FAMILY = Arrays.asList("LocationInfo");

    @Inject
    protected Provider<AbstractBackupPath> pathFactory;
    @Inject
    protected IBackupFileSystem fs;

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
