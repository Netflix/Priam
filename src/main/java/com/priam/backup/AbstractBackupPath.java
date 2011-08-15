package com.priam.backup;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;

import com.google.inject.Inject;
import com.priam.conf.IConfiguration;
import com.priam.identity.InstanceIdentity;
import com.priam.utils.RandomAccessReader;

public abstract class AbstractBackupPath implements Comparable<AbstractBackupPath>
{
    public static final SimpleDateFormat DAY_FORMAT = new SimpleDateFormat("yyyyMMddHHmm");
    public static final char PATH_SEP = '/';

    public static enum BackupFileType
    {
        SNAP, SST, CL, META, RANGE
    };

    public BackupFileType type;
    public String clusterName;
    public String keyspace;
    public String fileName;
    public String baseDir;
    public String token;
    public String region;
    public Date time;
    // TODO cleanup... don't need this?
    private File backupFile;

    @Inject
    public InstanceIdentity factory;
    @Inject
    public IConfiguration config;

    public SimpleDateFormat getFormat()
    {
        return DAY_FORMAT;
    }

    public RandomAccessReader localReader() throws IOException
    {
        assert backupFile != null;
        return RandomAccessReader.open(backupFile, true);
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
        this.token = factory.getInstance().getPayload();
        this.type = type;
        if (type != BackupFileType.META)
            this.keyspace = elements[0];
        if (type == BackupFileType.SNAP)
            time = DAY_FORMAT.parse(elements[2]);
        if (type == BackupFileType.SST)
            time = new Date(file.lastModified());
        this.fileName = file.getName();
    }

    /**
     * TODO cleanup... Pass Dates or String here?
     */
    public String match(Date start, Date end)
    {
        String sString = DAY_FORMAT.format(start);
        String eString = DAY_FORMAT.format(end);
        int diff = StringUtils.indexOfDifference(sString, eString);
        return sString.substring(0, diff);
    }

    /**
     * Restore location locally...
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
        return time.compareTo(o.time);
    }

    public abstract String getRemotePath();

    public abstract void parseRemote(String remoteFilePath);

    public abstract String remotePrefix(Date start, Date end);
}
