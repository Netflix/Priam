package com.netflix.priam.notification;

import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.config.IBackupRestoreConfig;
import com.netflix.priam.config.IConfiguration;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Instant;
import java.util.Map;
import javax.inject.Provider;
import mockit.Capturing;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Before;
import org.junit.Test;

public class TestBackupNotificationMgr {
    private Injector injector;
    private BackupNotificationMgr backupNotificationMgr;
    private Provider<AbstractBackupPath> abstractBackupPathProvider;
    private IConfiguration configuration;

    @Before
    public void setUp() {
        if (injector == null) {
            injector = Guice.createInjector(new BRTestModule());
        }

        if (backupNotificationMgr == null) {
            backupNotificationMgr = injector.getInstance(BackupNotificationMgr.class);
        }

        if (abstractBackupPathProvider == null) {
            abstractBackupPathProvider = injector.getProvider(AbstractBackupPath.class);
        }
    }

    @Test
    public void testNotificationNonEmptyFilter(
            @Mocked IBackupRestoreConfig backupRestoreConfig,
            @Capturing INotificationService notificationService)
            throws ParseException {
        new Expectations() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                result = "SNAPSHOT_VERIFIED, META_V2";
                maxTimes = 2;
            }
        };
        new Expectations() {
            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 1;
            }
        };
        Path path =
                Paths.get(
                        "fakeDataLocation",
                        "fakeKeyspace",
                        "fakeColumnFamily",
                        "fakeBackup",
                        "fakeData.db");
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        abstractBackupPath.parseLocal(path.toFile(), AbstractBackupPath.BackupFileType.META_V2);
        backupNotificationMgr.notify(abstractBackupPath, UploadStatus.STARTED);
        new Verifications() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                maxTimes = 2;
            }

            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 1;
            }
        };
    }

    @Test
    public void testNoNotificationsNonEmptyFilter(
            @Mocked IBackupRestoreConfig backupRestoreConfig,
            @Capturing INotificationService notificationService)
            throws ParseException {
        new Expectations() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                result = "META_V2";
                maxTimes = 2;
            }
        };
        new Expectations() {
            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 0;
            }
        };
        Path path =
                Paths.get(
                        "fakeDataLocation",
                        "fakeKeyspace",
                        "fakeColumnFamily",
                        "fakeBackup",
                        "fakeData.db");
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        abstractBackupPath.parseLocal(path.toFile(), AbstractBackupPath.BackupFileType.SST);
        backupNotificationMgr.notify(abstractBackupPath, UploadStatus.STARTED);
        new Verifications() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                maxTimes = 2;
            }

            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 0;
            }
        };
    }

    @Test
    public void testNotificationsEmptyFilter(
            @Mocked IBackupRestoreConfig backupRestoreConfig,
            @Capturing INotificationService notificationService)
            throws ParseException {
        new Expectations() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                result = "";
                maxTimes = 1;
            }
        };
        new Expectations() {
            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 1;
            }
        };
        Path path =
                Paths.get(
                        "fakeDataLocation",
                        "fakeKeyspace",
                        "fakeColumnFamily",
                        "fakeBackup",
                        "fakeData.db");
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        abstractBackupPath.parseLocal(path.toFile(), AbstractBackupPath.BackupFileType.SST);
        backupNotificationMgr.notify(abstractBackupPath, UploadStatus.STARTED);
        new Verifications() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                maxTimes = 1;
            }

            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 1;
            }
        };
    }

    @Test
    public void testNotificationsInvalidFilter(
            @Mocked IBackupRestoreConfig backupRestoreConfig,
            @Capturing INotificationService notificationService)
            throws ParseException {
        new Expectations() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                result = "SOME_FAKE_FILE_TYPE_1, SOME_FAKE_FILE_TYPE_2";
                maxTimes = 2;
            }
        };
        new Expectations() {
            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 1;
            }
        };
        Path path =
                Paths.get(
                        "fakeDataLocation",
                        "fakeKeyspace",
                        "fakeColumnFamily",
                        "fakeBackup",
                        "fakeData.db");
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        abstractBackupPath.parseLocal(path.toFile(), AbstractBackupPath.BackupFileType.SST);
        backupNotificationMgr.notify(abstractBackupPath, UploadStatus.STARTED);
        new Verifications() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                maxTimes = 2;
            }

            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 1;
            }
        };
    }

    @Test
    public void testNotificationsPartiallyValidFilter(
            @Mocked IBackupRestoreConfig backupRestoreConfig,
            @Capturing INotificationService notificationService)
            throws ParseException {
        new Expectations() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                result = "SOME_FAKE_FILE_TYPE_1, SOME_FAKE_FILE_TYPE_2, META_V2";
                maxTimes = 2;
            }
        };
        new Expectations() {
            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 1;
            }
        };
        Path path =
                Paths.get(
                        "fakeDataLocation",
                        "fakeKeyspace",
                        "fakeColumnFamily",
                        "fakeBackup",
                        "fakeData.db");
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        abstractBackupPath.parseLocal(path.toFile(), AbstractBackupPath.BackupFileType.META_V2);
        backupNotificationMgr.notify(abstractBackupPath, UploadStatus.STARTED);
        new Verifications() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                maxTimes = 2;
            }

            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 1;
            }
        };
    }

    @Test
    public void testNoNotificationsPartiallyValidFilter(
            @Mocked IBackupRestoreConfig backupRestoreConfig,
            @Capturing INotificationService notificationService)
            throws ParseException {
        new Expectations() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                result = "SOME_FAKE_FILE_TYPE_1, SOME_FAKE_FILE_TYPE_2, SST";
                maxTimes = 2;
            }
        };
        new Expectations() {
            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 0;
            }
        };
        Path path =
                Paths.get(
                        "fakeDataLocation",
                        "fakeKeyspace",
                        "fakeColumnFamily",
                        "fakeBackup",
                        "fakeData.db");
        AbstractBackupPath abstractBackupPath = abstractBackupPathProvider.get();
        abstractBackupPath.parseLocal(path.toFile(), AbstractBackupPath.BackupFileType.META_V2);
        backupNotificationMgr.notify(abstractBackupPath, UploadStatus.STARTED);
        new Verifications() {
            {
                backupRestoreConfig.getBackupNotifyComponentIncludeList();
                maxTimes = 2;
            }

            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 0;
            }
        };
    }

    @Test
    public void testNotify(@Capturing INotificationService notificationService) {
        new Expectations() {
            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 1;
            }
        };
        backupNotificationMgr.notify("some_random", Instant.EPOCH);
        new Verifications() {
            {
                notificationService.notify(anyString, (Map<String, MessageAttributeValue>) any);
                maxTimes = 1;
            }
        };
    }
}
