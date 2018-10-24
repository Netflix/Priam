package com.netflix.priam.backup

import com.netflix.priam.config.FakeConfiguration
import com.netflix.priam.scheduler.SchedulerType
import com.netflix.priam.scheduler.UnsupportedTypeException
import spock.lang.Specification
import spock.lang.Unroll

/**
 Created by aagrawal on 11/7/17.
 */
@Unroll
class TestBackupScheduler extends Specification {
    def "IsBackupEnabled for SchedulerType #schedulerType with hour #configHour and CRON #configCRON is #result"() {
        expect:
        SnapshotBackup.isBackupEnabled(new BackupConfiguration(schedulerType, configCRON, configHour)) == result

        where:
        schedulerType | configCRON        | configHour  || result
        "hour"        | null              | -1          || false
        "hour"        | "0 0 9 1/1 * ? *" | -1          || false
        "hour"        | null              | 1           || true
        "cron"        | "-1"              | 1           || false
        "cron"        | "-1"              | -1          || false
        "cron"        | "0 0 9 1/1 * ? *" | -1          || true
    }

    def "Exception for illegal value of Snapshot CRON expression , #configCRON"() {
        when:
        SnapshotBackup.isBackupEnabled(new BackupConfiguration("cron", configCRON, 1))

        then:
        thrown(expectedException)

        where:
        configCRON || expectedException
        "abc" || Exception
        "0 9 1/1 * ? *"|| Exception
    }

    def "Validate CRON for backup for SchedulerType #schedulerType with hour #configHour and CRON #configCRON is #result"() {
        expect:
        SnapshotBackup.getTimer(new BackupConfiguration(schedulerType, configCRON, configHour)).cronExpression == result

        where:
        schedulerType | configCRON        | configHour  || result
        "hour"        | null              | 1           ||  "0 1 1 * * ?"
        "cron"        | "0 0 9 1/1 * ? *" | -1          ||  "0 0 9 1/1 * ? *"
    }

    private class BackupConfiguration extends FakeConfiguration {
        private String backupSchedulerType, backupCronExpression
        private int backupHour

        BackupConfiguration(String backupSchedulerType, String backupCronExpression, int backupHour) {
            this.backupCronExpression = backupCronExpression
            this.backupSchedulerType = backupSchedulerType
            this.backupHour = backupHour
        }

        @Override
        SchedulerType getBackupSchedulerType() throws UnsupportedTypeException {
            return SchedulerType.lookup(backupSchedulerType)
        }

        @Override
        String getBackupCronExpression() {
            return backupCronExpression
        }

        @Override
        int getBackupHour() {
            return backupHour
        }
    }

}
