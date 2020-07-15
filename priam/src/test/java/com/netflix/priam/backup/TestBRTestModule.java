package com.netflix.priam.backup;

import com.google.common.truth.Truth;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.merics.BackupMetrics;
import org.junit.Before;
import org.junit.Test;

public class TestBRTestModule {
    private Injector injector;

    @Before
    public void setInjector() {
        injector = Guice.createInjector(new BRTestModule());
    }

    @Test
    public void injectBackupMetrics() {
        Truth.assertThat(injector.getInstance(BackupMetrics.class)).isNotNull();
    }
}
