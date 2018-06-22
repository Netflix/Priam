package com.netflix.priam.restore;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.scheduler.NamedThreadPoolExecutor;
import com.netflix.priam.utils.RetryableCallable;
import com.netflix.priam.utils.Sleeper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of IPostRestoreHook. Kicks off a child process for post restore hook using ProcessBuilder; uses heart beat monitor to monitor progress of the sub process
 * and uses a file lock to pass the active state to the sub process
 */
public class PostRestoreHook implements IPostRestoreHook {
    private static final Logger logger = LoggerFactory.getLogger(PostRestoreHook.class);
    private final IConfiguration config;
    private final Sleeper sleeper;
    private final int ThreadWaitTimeInMs = 60000;
    private final int HeartBeatTimeOutInMs = 60000;
    private final int SubProcessTimeOutInDays = 2;
    private static String PostRestoreHookCommandDelimiter = " ";
    private static String PriamPostRestoreHookFilePrefix = "PriamFileForPostRestoreHook";
    private static String PriamPostRestoreHookFileSuffix = ".tmp";
    private static String PriamPostRestoreHookFileOptionName = "--parentHookFilePath=";

    @Inject
    public PostRestoreHook(IConfiguration config, Sleeper sleeper) {
        this.config = config;
        this.sleeper = sleeper;
    }

    /**
     * Checks parameters to make sure none are blank
     * @return if all parameters are valid
     */
    public boolean hasValidParameters() {
        if(config.isPostRestoreHookEnabled()) {
            if(StringUtils.isBlank(config.getPostRestoreHook())
                    || StringUtils.isBlank(config.getPostRestoreHookHeartbeatFileName())
                    || StringUtils.isBlank(config.getPostRestoreHookDoneFileName())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Executes a sub process as part of post restore hook, and waits for the completion of the process. In case of lack of heart beat from the sub process, existing sub process is terminated
     * and new sub process is kicked off
     * @throws Exception
     */
    public void execute() throws Exception {
        if(config.isPostRestoreHookEnabled()) {
            logger.info("Started PostRestoreHook execution");

            //create a temp file to be used to indicate state of the current process, to the sub-process
            File tempLockFile = File.createTempFile(PriamPostRestoreHookFilePrefix, PriamPostRestoreHookFileSuffix);
            RandomAccessFile raf = new RandomAccessFile(tempLockFile.getPath(), "rw");
            FileChannel fileChannel = raf.getChannel();
            FileLock lock = fileChannel.lock();

            try {
                if (lock.isValid()) {
                    logger.info("Lock on RestoreHookFile acquired");
                    int countOfProcessStarts = 0;
                    while (true) {
                        if (doneFileExists()) {
                            logger.info("Not starting PostRestoreHook since DONE file already exists.");
                            break;
                        }

                        String postRestoreHook = config.getPostRestoreHook();
                        //add temp file path as parameter to the jar file
                        postRestoreHook = postRestoreHook + PostRestoreHookCommandDelimiter + PriamPostRestoreHookFileOptionName + tempLockFile.getAbsolutePath();
                        String[] processCommandArguments = postRestoreHook.split(PostRestoreHookCommandDelimiter);
                        ProcessBuilder processBuilder = new ProcessBuilder(processCommandArguments);

                        //start sub-process
                        Process process = processBuilder.inheritIO().start();
                        logger.info("Started PostRestoreHook - Attempt#{}", ++countOfProcessStarts);

                        //monitor progress of sub-process
                        monitorPostRestoreHookHeartBeat(process);

                        //block until sub-process completes or until the timeout
                        if (!process.waitFor(SubProcessTimeOutInDays, TimeUnit.DAYS)) {
                            logger.info("PostRestoreHook process did not complete within {} days. Forcefully terminating the process.", SubProcessTimeOutInDays);
                            process.destroyForcibly();
                        }

                        if (process.exitValue() == 0) {
                            logger.info("PostRestoreHook process completed successfully");
                            break;
                        }
                        logger.info("PostRestoreHook process exited unsuccessfully");
                    }
                    logger.info("Completed PostRestoreHook execution");
                } else {
                    throw new PostRestoreHookException(String.format("Could not acquire lock on a temp file necessary for PostRestoreHook to execute. Path to temp file: %s", tempLockFile.getAbsolutePath()));
                }
            } finally {
                //close and delete temp file
                lock.release();
                fileChannel.close();
                raf.close();
                tempLockFile.delete();
            }
        }
    }


    /**
     * Monitors heart beat of the process
     * @param process Process to be monitored
     * @throws InterruptedException
     * @throws IOException
     */
    private void monitorPostRestoreHookHeartBeat(Process process) throws InterruptedException, IOException {
        File heartBeatFile = new File(config.getPostRestoreHookHeartbeatFileName());
        ThreadPoolExecutor heartBeatPoolExecutor = new NamedThreadPoolExecutor(1, "PostRestoreHook_HeartBeatThreadPool");
        heartBeatPoolExecutor.allowCoreThreadTimeOut(true);
        heartBeatPoolExecutor.submit(new RetryableCallable<Integer>() {
            @Override
            public Integer retriableCall() throws Exception {
                while (true) {
                    sleeper.sleep(ThreadWaitTimeInMs);
                    if(System.currentTimeMillis() - heartBeatFile.lastModified() > HeartBeatTimeOutInMs) {
                        //kick off post restore hook process, since there is no heartbeat
                        logger.info("No heartbeat for the last {} ms, killing the existing process.", HeartBeatTimeOutInMs);
                        if(process.isAlive()) {
                            process.destroyForcibly();
                        }
                        return 0;
                    }
                }
            }
        });
    }

    /**
     * Checks for presence of DONE file
     * @return if done file exists
     */
    private boolean doneFileExists() {
        File doneFile = new File(config.getPostRestoreHookDoneFileName());
        return doneFile.exists();
    }
}
