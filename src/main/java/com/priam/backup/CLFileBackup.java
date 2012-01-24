package com.priam.backup;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.priam.backup.AbstractBackupPath.BackupFileType;
import com.priam.backup.CLStreamBackup.CL_REQ_TYPE;
import com.priam.conf.IConfiguration;
import com.priam.scheduler.SimpleTimer;
import com.priam.scheduler.TaskTimer;
import com.priam.utils.RetryableCallable;

/**
 * Look for backup files that were missed by current commit log stream. 
 * @author Praveen Sadhu
 *
 */
@Singleton
public class CLFileBackup extends Backup
{
    private static final Logger logger = LoggerFactory.getLogger(CLFileBackup.class);
    public static final String JOBNAME = "CL_FILE_BACKUP";

    private IConfiguration config;

    @Inject
    public CLFileBackup(IConfiguration config)
    {
        this.config = config;
    }

    @Override
    public void execute() throws Exception
    {
        File clDir = new File(config.getCommitLogLocation());
        logger.debug("Scanning for commit logs in: " + clDir.getAbsolutePath());
        for (File clFile : clDir.listFiles())
        {
            // Check if file exists
            if (clFile.getName().equals(CLStreamBackup.fileName))
                continue;
            AbstractBackupPath bp = pathFactory.get();
            bp.parseLocal(clFile, BackupFileType.CL);
            if (!remoteFileExists(bp))
                upload(bp);
        }
    }

    private boolean remoteFileExists(AbstractBackupPath bp)
    {
        Iterator<AbstractBackupPath> itr = fs.list(config.getBackupPrefix(), bp.time, bp.time);
        while (itr.hasNext())
        {
            AbstractBackupPath p = itr.next();
            if (p.getRemotePath().equals(bp.getRemotePath()))
                return true;
        }
        return false;
    }

    private void upload(final AbstractBackupPath path) throws Exception
    {
        new RetryableCallable<Void>()
        {
            public Void retriableCall() throws Exception
            {
                logger.info("Uploading: " + path.getRemotePath());
                Socket bSocket = new Socket(InetAddress.getLocalHost().getHostAddress(), config.getCommitLogBackupPort());
                bSocket.setTcpNoDelay(true);
                DataInputStream in = new DataInputStream(bSocket.getInputStream());
                DataOutputStream out = new DataOutputStream(bSocket.getOutputStream());
                out.writeInt(CL_REQ_TYPE.BACKUP_FILE.ordinal());
                out.writeUTF(config.getCommitLogLocation() + AbstractBackupPath.PATH_SEP + path.fileName);
                out.flush();
                fs.upload(path, new CLStreamBackup.CommitLogInputStream(in));
                return null;
            }
        }.call();

    }

    public static TaskTimer getTimer()
    {
        return new SimpleTimer(JOBNAME, 60L * 1000);
    }

    @Override
    public String getName()
    {
        return JOBNAME;
    }

}
