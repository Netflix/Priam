package com.netflix.priam.cli;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.Restore;

public class Restorer
{
    private static final Logger logger = LoggerFactory.getLogger(Restorer.class);

    static void displayHelp()
    {
        System.out.println("Usage: command_name FROM_DATE TO_DATE");
    }

    public static void main(String[] args)
    {
        try
        {
            Application.initialize();

            Date startTime, endTime;
            if (args.length < 2)
            {
                displayHelp();
                return;
            }
            AbstractBackupPath path = Application.getInjector().getInstance(AbstractBackupPath.class);
            startTime = path.parseDate(args[0]);
            endTime = path.parseDate(args[1]);

            Restore restorer = Application.getInjector().getInstance(Restore.class);
            try
            {
                restorer.restore(startTime, endTime);
            } catch (Exception e)
            {
                logger.error("Unable to restore: ", e);
            }
        } finally
        {
            Application.shutdownAdditionalThreads();
        }
    }
}