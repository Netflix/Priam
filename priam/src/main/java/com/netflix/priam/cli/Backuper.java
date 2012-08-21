package com.netflix.priam.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.backup.SnapshotBackup;

public class Backuper
{
    private static final Logger logger = LoggerFactory.getLogger(Backuper.class);

    public static void main(String[] args)
    {
        Injector injector = Guice.createInjector(new LightGuiceModule());
        IConfiguration conf = injector.getInstance(IConfiguration.class);
        conf.intialize();
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        SnapshotBackup backuper = injector.getInstance(SnapshotBackup.class);
        try
        {
            backuper.execute();
        } catch (Exception e)
        {
            logger.error("Unable to backup: ", e);
        }
        fs.shutdown();
    }
}