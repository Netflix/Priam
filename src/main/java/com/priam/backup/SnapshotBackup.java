package com.priam.backup;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.priam.backup.AbstractBackupPath.BackupFileType;
import com.priam.conf.IConfiguration;
import com.priam.conf.JMXNodeTool;
import com.priam.scheduler.CronTimer;
import com.priam.scheduler.TaskTimer;

public class SnapshotBackup extends Backup
{
    private static final Logger logger = LoggerFactory.getLogger(SnapshotBackup.class);
    public static String JOBNAME = "SnapshotBackup";
    private IConfiguration config;
    private String error;
    private MetaData metaData;

    @Inject
    public SnapshotBackup(IConfiguration config, MetaData metaData) throws IOException, InterruptedException
    {
        this.config = config;
        this.metaData = metaData;
    }

    @Override
    public void execute() throws Exception
    {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        String snapshotName = pathFactory.get().getFormat().format(cal.getTime());
        try
        {
            takeSnapshot(snapshotName);
            // Collect all snapshot dir's under keyspace dir's
            List<AbstractBackupPath> bps = Lists.newArrayList();
            File dataDir = new File(config.getDataFileLocation());
            File[] keyspaceDirs = dataDir.listFiles();
            for (File keyspaceDir : keyspaceDirs)
            {
                File snpDir = new File(keyspaceDir, "snapshots");
                if (!isValidBackupDir(keyspaceDir, snpDir))
                    continue;
                File snapshotDir = getValidateSnapshot(keyspaceDir, snpDir, snapshotName);
                // Add files to this dir
                if (null != snapshotDir)
                    bps.addAll(upload(snapshotDir, BackupFileType.SNAP));
            }
            // Upload meta file
            metaData.set(bps, snapshotName);
        }
        finally
        {
            try
            {
                clearSnapshot(snapshotName);
            }
            catch (Exception e)
            {
                logger.error(error, e);
            }
        }
    }

    private File getValidateSnapshot(File keyspaceDir, File snpDir, String snapshotName)
    {
        for (File snapshotDir : snpDir.listFiles())
            if (snapshotDir.getName().matches(snapshotName))
                return snapshotDir;
        return null;
    }

    private void takeSnapshot(String snapshotName) throws IOException, InterruptedException
    {
        JMXNodeTool nodetool = JMXNodeTool.instance(config);
        try
        {
            nodetool.takeSnapshot(snapshotName);
        }
        finally
        {
            nodetool.close();
        }
    }

    private void clearSnapshot(String snapshotTag) throws IOException, InterruptedException
    {
        JMXNodeTool nodetool = JMXNodeTool.instance(config);
        try
        {
            nodetool.clearSnapshot(snapshotTag);
        }
        finally
        {
            nodetool.close();
        }
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public static TaskTimer getTimer(IConfiguration config)
    {
        int hour = config.getBackupHour();
        return new CronTimer(hour, 1, 0);
    }
}
