package com.netflix.priam.cli;

import java.util.Date;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.aws.S3FileSystem;
import com.netflix.priam.backup.Restore;
import com.netflix.priam.backup.AbstractBackupPath;

public class Restorer
{
    private static final Logger logger = LoggerFactory.getLogger(Restorer.class);

    static void displayHelp()
    {
        System.out.println("Usage: command_name FROM_DATE TO_DATE");
    }

    public static void main(String[] args)
    {
        Injector injector = Guice.createInjector(new LightGuiceModule());

        IConfiguration conf = injector.getInstance(IConfiguration.class);
        conf.intialize();

        Date startTime, endTime;
        if (args.length < 2)
        {
            displayHelp();
            return;
        }
        try
        {
            AbstractBackupPath path = injector.getInstance(AbstractBackupPath.class);
            startTime = path.getFormat().parse(args[0]);
            endTime = path.getFormat().parse(args[1]);
        } catch (ParseException e)
        {
            logger.error("Unable to parse: ", e);
            displayHelp();
            return;
        }

        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        Restore restorer = injector.getInstance(Restore.class);
        try
        {
            restorer.restore(startTime, endTime);
        } catch (Exception e)
        {
            logger.error("Unable to restore: ", e);
        }
        fs.shutdown();
    }
}