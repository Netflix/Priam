package com.netflix.priam.tuner;

import com.netflix.priam.backup.IncrementalBackup;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import com.netflix.priam.connection.JMXNodeTool;
import com.netflix.priam.defaultimpl.IService;
import com.netflix.priam.scheduler.PriamScheduler;
import com.netflix.priam.utils.RetryableCallable;
import javax.inject.Inject;

public class CassandraTunerService implements IService {
    private final PriamScheduler scheduler;
    private final IConfiguration configuration;
    private final IBackupRestoreConfig backupRestoreConfig;

    @Inject
    public CassandraTunerService(
            PriamScheduler priamScheduler,
            IConfiguration configuration,
            IBackupRestoreConfig backupRestoreConfig) {
        this.scheduler = priamScheduler;
        this.configuration = configuration;
        this.backupRestoreConfig = backupRestoreConfig;
    }

    @Override
    public void scheduleService() throws Exception {
        // Run the task to tune Cassandra
        scheduler.runTaskNow(TuneCassandra.class);
    }

    @Override
    public void updateServicePre() throws Exception {}

    @Override
    public void updateServicePost() throws Exception {
        // Update the cassandra to enable/disable new incremental files.
        new RetryableCallable<Void>(6, 10000) {
            public Void retriableCall() throws Exception {
                try (JMXNodeTool nodeTool = JMXNodeTool.instance(configuration)) {
                    nodeTool.setIncrementalBackupsEnabled(
                            IncrementalBackup.isEnabled(configuration, backupRestoreConfig));
                }
                return null;
            }
        }.call();
    }
}
