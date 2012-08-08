package com.netflix.priam.backup;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.PriamServer;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.config.AmazonConfiguration;
import com.netflix.priam.config.BackupConfiguration;
import com.netflix.priam.config.CassandraConfiguration;
import com.netflix.priam.scheduler.SimpleTimer;
import com.netflix.priam.scheduler.TaskTimer;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import com.netflix.priam.utils.SystemUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Main class for restoring data from backup
 */
@Singleton
public class Restore extends AbstractRestore
{
    public static final String JOBNAME = "AUTO_RESTORE_JOB";
    private static final Logger logger = LoggerFactory.getLogger(Restore.class);

    @Inject private Provider<AbstractBackupPath> pathProvider;
    @Inject private RestoreTokenSelector tokenSelector;
    @Inject private MetaData metaData;
    @Inject private PriamServer priamServer;
    @Inject private CassandraConfiguration cassandraConfiguration;
    @Inject private AmazonConfiguration amazonConfiguration;

    @Inject
    public Restore(BackupConfiguration backupConfiguration, Sleeper sleeper)
    {
        super(backupConfiguration, JOBNAME, sleeper);
    }

    @Override
    public void execute() throws Exception
    {
        if (isRestoreEnabled(backupConfiguration, amazonConfiguration.getAvailabilityZone()))
        {
            logger.info("Starting restore for " + backupConfiguration.getAutoRestoreSnapshotName());
            String[] restore = backupConfiguration.getAutoRestoreSnapshotName().split(",");
            AbstractBackupPath path = pathProvider.get();
            final Date startTime = path.getFormat().parse(restore[0]);
            final Date endTime = path.getFormat().parse(restore[1]);
            String origToken = priamServer.getInstanceIdentity().getInstance().getToken();
            try
            {
                if (backupConfiguration.isRestoreClosestToken())
                {
                    restoreToken = tokenSelector.getClosestToken(new BigInteger(origToken), startTime);
                    priamServer.getInstanceIdentity().getInstance().setToken(restoreToken.toString());
                }
                new RetryableCallable<Void>()
                {
                    public Void retriableCall() throws Exception
                    {
                        logger.info("Attempting restore");
                        restore(startTime, endTime);
                        logger.info("Restore completed");
                        // Wait for other server init to complete
                        sleeper.sleep(30000);
                        return null;
                    }
                }.call();
            }
            finally
            {
                priamServer.getInstanceIdentity().getInstance().setToken(origToken);
            }
        }
        SystemUtils.startCassandra(true, cassandraConfiguration, backupConfiguration, amazonConfiguration.getInstanceType());
    }

    /**
     * Restore backup data for the specified time range
     */
    public void restore(Date startTime, Date endTime) throws Exception
    {
        // Stop cassandra if its running and restoring all keyspaces
        if (backupConfiguration.getRestoreKeyspaces().size() == 0)
            SystemUtils.stopCassandra(cassandraConfiguration);

        // Cleanup local data
        SystemUtils.cleanupDir(cassandraConfiguration.getDataLocation(), backupConfiguration.getRestoreKeyspaces());

        // Try and read the Meta file.
        List<AbstractBackupPath> metas = Lists.newArrayList();
        String prefix = StringUtils.isNotBlank(backupConfiguration.getRestorePrefix()) ? backupConfiguration.getRestorePrefix() : backupConfiguration.getS3BucketName();
        logger.info("Looking for meta file here:  " + prefix);
        Iterator<AbstractBackupPath> backupfiles = fs.list(prefix, startTime, endTime);
        while (backupfiles.hasNext())
        {
            AbstractBackupPath path = backupfiles.next();
            if (path.type == BackupFileType.META)
                metas.add(path);
        }
        assert metas.size() != 0 : "[cass_backup] No snapshots found, Restore Failed.";

        Collections.sort(metas);
        AbstractBackupPath meta = Iterators.getLast(metas.iterator());
        logger.info("Meta file for restore " + meta.getRemotePath());

        // Download snapshot which is listed in the meta file.
        List<AbstractBackupPath> snapshots = metaData.get(meta);
        download(snapshots.iterator(), BackupFileType.SNAP);

        logger.info("Downloading incrementals");
        // Download incrementals (SST).
        Iterator<AbstractBackupPath> incrementals = fs.list(prefix, meta.time, endTime);
        download(incrementals, BackupFileType.SST);
    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

    public int getActiveCount()
    {
        return (executor == null) ? 0 : executor.getActiveCount();
    }

    public static boolean isRestoreEnabled(BackupConfiguration conf, String availabilityZone)
    {
        boolean isRestoreMode = StringUtils.isNotBlank(conf.getAutoRestoreSnapshotName());
        boolean isBackedupRac = (CollectionUtils.isEmpty(conf.getAvailabilityZonesToBackup()) || conf.getAvailabilityZonesToBackup().contains(availabilityZone));
        return (isRestoreMode && isBackedupRac);
    }

}
