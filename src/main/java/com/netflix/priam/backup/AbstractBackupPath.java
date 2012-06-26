package com.netflix.priam.backup;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;

import com.netflix.priam.IConfiguration;
import com.netflix.priam.identity.InstanceIdentity;

public abstract class AbstractBackupPath implements Comparable<AbstractBackupPath>
{
    public static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");
    public static final char PATH_SEP = '/';
    public static final Pattern clPattern = Pattern.compile(".*CommitLog-(\\d{13}).log");

    public static enum BackupFileType
    {
        SNAP, SST, CL, META
    };

    protected BackupFileType type;
    protected String clusterName;
    protected String keyspace;
    protected String fileName;
    protected String baseDir;
    protected String token;
    protected String region;
    protected Date time;
    protected long size;

    protected final InstanceIdentity factory;
    protected final IConfiguration config;
    protected File backupFile;

    public AbstractBackupPath(IConfiguration config, InstanceIdentity factory)
    {
        this.factory = factory;
        this.config = config;
    }

    public SimpleDateFormat getFormat()
    {
        return DAY_FORMAT;
    }

    public InputStream localReader() throws IOException
    {
        assert backupFile != null;
        return new RafInputStream(RandomAccessReader.open(backupFile, true));
    }

    public void parseLocal(File file, BackupFileType type) throws ParseException
    {
        // TODO cleanup.
        this.backupFile = file;

        String rpath = new File(config.getDataFileLocation()).toURI().relativize(file.toURI()).getPath();
        String[] elements = rpath.split("" + PATH_SEP);
        this.clusterName = config.getAppName();
        this.baseDir = config.getBackupLocation();
        this.region = config.getDC();
        this.token = factory.getInstance().getToken();
        this.type = type;
        if (type != BackupFileType.META && type != BackupFileType.CL)
            this.keyspace = elements[0];
        if (type == BackupFileType.SNAP)
            time = DAY_FORMAT.parse(elements[3]);
        if (type == BackupFileType.SST || type == BackupFileType.CL)
            time = new Date(file.lastModified());
        this.fileName = file.getName();
        this.size = file.length();
    }

    /**
     * Given a date range, find a common string prefix Eg: 20120212, 20120213 =>
     * 2012021
     */
    public String match(Date start, Date end)
    {
        String sString = DAY_FORMAT.format(start);
        String eString = DAY_FORMAT.format(end);
        int diff = StringUtils.indexOfDifference(sString, eString);
        if (diff < 0)
            return sString;
        return sString.substring(0, diff);
    }

    /**
     * Local restore file
     */
    public File newRestoreFile()
    {
        StringBuffer buff = new StringBuffer();
        buff.append(config.getDataFileLocation()).append(PATH_SEP);
        if (type != BackupFileType.META)
            buff.append(keyspace).append(PATH_SEP);
        buff.append(fileName);
        File return_ = new File(buff.toString());
        File parent = new File(return_.getParent());
        if (!parent.exists())
            parent.mkdirs();
        return return_;
    }

    @Override
    public int compareTo(AbstractBackupPath o)
    {
        return getRemotePath().compareTo(o.getRemotePath());
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (!obj.getClass().equals(this.getClass()))
            return false;
        return getRemotePath().equals(((AbstractBackupPath)obj).getRemotePath());
    }

    /**
     * Get remote prefix for this path object
     */
    public abstract String getRemotePath();

    /**
     * Parses a fully constructed remote path
     */
    public abstract void parseRemote(String remoteFilePath);

    /**
     *  Parses paths with just token prefixes 
     */
    public abstract void parsePartialPrefix(String remoteFilePath);

    /**
     * Provides a common prefix that matches all objects that fall between 
     * the start and end time 
     */
    public abstract String remotePrefix(Date start, Date end, String location);
    
    /**
     * Provides the cluster prefix
     */
    public abstract String clusterPrefix(String location);

    public BackupFileType getType()
    {
        return type;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getFileName()
    {
        return fileName;
    }

    public String getBaseDir()
    {
        return baseDir;
    }

    public String getToken()
    {
        return token;
    }

    public String getRegion()
    {
        return region;
    }

    public Date getTime()
    {
        return time;
    }

    public long getSize()
    {
        return size;
    }

    public File getBackupFile()
    {
        return backupFile;
    }

    public static class RafInputStream extends InputStream
    {
        private RandomAccessFile raf;

        public RafInputStream(RandomAccessFile raf)
        {
            this.raf = raf;
        }

        @Override
        public synchronized int read(byte[] bytes, int off, int len) throws IOException
        {
            return raf.read(bytes, off, len);
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(raf);
        }

        @Override
        public int read() throws IOException
        {
            return 0;
        }
    }

    @Override
    public String toString()
    {
        return "From: " + getRemotePath() + " To: " + newRestoreFile().getPath();
    }
}
