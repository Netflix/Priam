package com.netflix.priam.backup

import com.netflix.priam.config.FakeConfiguration
import spock.lang.Specification
import spock.lang.Unroll

/**
 Created by aagrawal on 11/7/17.
 */
@Unroll
class TestBackupScheduler extends Specification {
    def "IsBackupEnabled CRON #configCRON is #result"() {
        expect:
        SnapshotBackup.isBackupEnabled(new BackupConfiguration(configCRON)) == result

        where:
        configCRON        || result
        "-1"              || false
        "0 0 9 1/1 * ? *" || true
    }

    def "Exception for illegal value of Snapshot CRON expression , #configCRON"() {
        when:
        SnapshotBackup.isBackupEnabled(new BackupConfiguration(configCRON))

        then:
        thrown(expectedException)

        where:
        configCRON || expectedException
        "abc" || Exception
        "0 9 1/1 * ? *"|| Exception
    }

    def "Validate CRON for backup CRON #configCRON is #result"() {
        expect:
        SnapshotBackup.getTimer(new BackupConfiguration(configCRON)).cronExpression == result

        where:
        configCRON        || result
        "0 0 9 1/1 * ? *" ||  "0 0 9 1/1 * ? *"
    }

    private class BackupConfiguration extends FakeConfiguration {
        private String backupCronExpression

        BackupConfiguration(String backupCronExpression) {
            this.backupCronExpression = backupCronExpression
        }

        @Override
        String getBackupCronExpression() {
            return backupCronExpression
        }
    }

}
