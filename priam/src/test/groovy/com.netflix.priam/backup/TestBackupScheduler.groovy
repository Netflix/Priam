/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
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
