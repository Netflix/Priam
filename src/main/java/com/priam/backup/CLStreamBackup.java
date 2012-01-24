package com.priam.backup;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.priam.backup.AbstractBackupPath.BackupFileType;
import com.priam.conf.IConfiguration;
import com.priam.scheduler.SimpleTimer;
import com.priam.scheduler.Task;
import com.priam.scheduler.TaskTimer;
import com.priam.utils.ExponentialRetryCallable;

/**
 * Get commit log stream from cassandra and backup
 * 
 * @author Praveen Sadhu
 * 
 */
@Singleton
public class CLStreamBackup extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(CLStreamBackup.class);
    public static final String JOBNAME = "CL_STREAM_BACKUP";
    public static final int INT_SIZE = 4;
    private IBackupFileSystem fs;
    public static String fileName;

    public enum CL_REQ_TYPE
    {
        BACKUP_FILE, BACKUP_CURRENT_CL, RESTORE;
    };

    @Inject
    protected Provider<AbstractBackupPath> pathFactory;

    @Inject
    protected IConfiguration config;

    @Inject
    public CLStreamBackup(IBackupFileSystem fs)
    {
        this.fs = fs;
    }

    @Override
    public void execute() throws Exception
    {
        new ExponentialRetryCallable<Void>()
        {
            @Override
            public Void retriableCall() throws Exception
            {
                Socket bSocket = null;
                DataInputStream in = null;
                DataOutputStream out = null;
                try
                {
                    logger.info("Connecting to " + InetAddress.getLocalHost().getHostAddress());
                    bSocket = new Socket(InetAddress.getLocalHost().getHostAddress(), config.getCommitLogBackupPort());
                    bSocket.setTcpNoDelay(true);
                    out = new DataOutputStream(bSocket.getOutputStream());
                    in = new DataInputStream(bSocket.getInputStream());
                    out.writeInt(CL_REQ_TYPE.BACKUP_CURRENT_CL.ordinal());
                    out.flush();
                    assert (in.readInt() == Integer.MAX_VALUE);
                    uploadFiles(in, out);
                }
                catch (Exception e)
                {
                    logger.error(e.getMessage(), e);
                    throw e;
                }
                finally
                {
                    FileUtils.closeQuietly(in);
                    FileUtils.closeQuietly(out);
                    bSocket.close();
                }
                return null;
            }
        }.call();
    }

    private void uploadFiles(DataInputStream in, DataOutputStream out) throws Exception
    {
        while (true)
        {
            fileName = in.readUTF();
            logger.info("New commit log " + fileName);
            AbstractBackupPath bp = pathFactory.get();
            bp.parseLocal(new File(config.getCommitLogLocation() + AbstractBackupPath.PATH_SEP + fileName), BackupFileType.CL);
            fs.upload(bp, new CommitLogInputStream(in));
        }
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

    public static OutputStream getOuputStream(IConfiguration config) throws UnknownHostException, IOException
    {
        return new CommitLogOuputStream(config);
    }

    public static class CommitLogInputStream extends InputStream
    {
        private DataInputStream dis;
        private int rem = 0;

        public CommitLogInputStream(DataInputStream dis) throws UnknownHostException, IOException
        {
            this.dis = dis;
        }

        @Override
        public synchronized int read(byte[] bytes, int off, int len) throws IOException
        {
            // Read length
            if (rem == 0)
            {
                rem = dis.readInt();
                return writeHeader(rem, bytes);
            }
            int min = Math.min(rem, len);
            min = dis.read(bytes, off, min);
            rem -= min;
            return min;
        }

        private int writeHeader(int size, byte[] bytes)
        {
            if (size == Integer.MAX_VALUE || size == 0)
                return -1;
            System.arraycopy(ByteBuffer.allocate(INT_SIZE).putInt(size).array(), 0, bytes, 0, INT_SIZE);
            return INT_SIZE;
        }

        @Override
        public int read() throws IOException
        {
            return 0;
        }
    }

    public static class CommitLogOuputStream extends OutputStream
    {
        private Socket rSocket;
        private DataOutputStream dos;

        public CommitLogOuputStream(IConfiguration config) throws UnknownHostException, IOException
        {
            logger.info("Connecting to " + InetAddress.getLocalHost().getHostAddress());
            rSocket = new Socket(InetAddress.getLocalHost().getHostAddress(), config.getCommitLogBackupPort());
            dos = new DataOutputStream(rSocket.getOutputStream());
            dos.writeInt(CL_REQ_TYPE.RESTORE.ordinal());
            dos.flush();
        }

        public void write(byte[] b, int off, int len) throws IOException
        {
            dos.write(b, off, len);
        }

        @Override
        public void write(int b) throws IOException
        {
            dos.write(b);
        }

        @Override
        public void close()
        {
            try
            {
                dos.writeInt(0);
                dos.close();
            }
            catch (IOException e)
            {
                logger.error(e.getMessage(), e);
            }
        }
    }

}
