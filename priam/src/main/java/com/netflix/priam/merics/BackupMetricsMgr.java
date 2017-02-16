package com.netflix.priam.merics;

import com.google.inject.Singleton;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.IBackupMetrics;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by vinhn on 2/13/17.
 */
@Singleton
public class BackupMetricsMgr implements IBackupMetrics{
    private AtomicInteger validUploads = new AtomicInteger()
            , invalidUploads = new AtomicInteger()
            , validDownloads = new AtomicInteger()
            , invalidDownloads = new AtomicInteger()
            ;

    @Override
    public void incrementValidUploads() {
        this.validUploads.getAndIncrement();
    }

    @Override
    public int getInvalidUploads() {
        return this.invalidUploads.get();
    }
    @Override
    public void incrementInvalidUploads() {
        this.invalidUploads.getAndIncrement();
    }

    @Override
    public int getValidUploads() {
        return this.validUploads.get();
    }

    @Override
    public int getValidDownloads() {
        return this.validDownloads.get();
    }

    @Override
    public void incrementValidDownloads() {
        this.invalidDownloads.getAndIncrement();
    }

    @Override
    public int getInvalidDownloads() {
        return this.invalidDownloads.get();
    }

    @Override
    public void incrementInvalidDownloads() {
        this.invalidDownloads.getAndIncrement();
    }

}
