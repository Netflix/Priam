package com.priam.aws;

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import com.google.common.collect.Lists;
import com.priam.backup.AbstractBackupPath;

public class S3BackupPath extends AbstractBackupPath
{
    /**
     * Format:
     * OLD:CLUSTER/BASE/[SNAPSHOT|TIMESTAMP]/[sst|snp]/TOKEN/REGION/KEYSPACE/FILE 
     * NEW:BASE/REGION/CLUSTER/TOKEN/[SNAPSHOTTIME]/[SST|SNP|META]/KEYSPACE/FILE
     */
    public String getRemotePath()
    {
        StringBuffer buff = new StringBuffer();
        buff.append(baseDir).append(S3BackupPath.PATH_SEP); // Base dir
        buff.append(region).append(S3BackupPath.PATH_SEP);
        buff.append(clusterName).append(S3BackupPath.PATH_SEP);// Cluster name
        buff.append(token).append(S3BackupPath.PATH_SEP);
        buff.append(getFormat().format(time)).append(S3BackupPath.PATH_SEP);
        buff.append(type).append(S3BackupPath.PATH_SEP);
        if (type != BackupFileType.META)
            buff.append(keyspace).append(S3BackupPath.PATH_SEP);
        buff.append(fileName);
        return buff.toString();
    }

    public void parseRemote(String remoteFilePath)
    {
        try
        {
            String[] elements = remoteFilePath.split(String.valueOf(S3BackupPath.PATH_SEP));
            // parse out things which are empty
            List<String> pieces = Lists.newArrayList();
            for (String ele : elements)
            {
                if (ele.equals(""))
                    continue;
                pieces.add(ele);
            }
            assert pieces.size() >= 7 : "Too few elements in path " + remoteFilePath;
            baseDir = pieces.get(0);
            region = pieces.get(1);
            clusterName = pieces.get(2);
            token = pieces.get(3);
            time = getFormat().parse(pieces.get(4));
            type = BackupFileType.valueOf(pieces.get(5));
            if (type != BackupFileType.META)
            {
                keyspace = pieces.get(6);
            }
            // append the rest
            fileName = pieces.get(pieces.size() - 1);
        }
        catch (ParseException e)
        {
            throw new RuntimeException();
        }
    }

    public String remotePrefix(Date start, Date end, String location)
    {
        StringBuffer buff = new StringBuffer();
        String[] elements = location.split(String.valueOf(S3BackupPath.PATH_SEP));          
        if (elements.length <= 1)
        {
            baseDir = config.getBackupLocation();
            region = config.getDC();
            clusterName = config.getAppName();
        }
        else
        {
            assert elements.length >= 4 : "Too few elements in path " + location;
            baseDir = elements[1];
            region = elements[2];
            clusterName = elements[3];
        }
        buff.append(baseDir).append(S3BackupPath.PATH_SEP);
        buff.append(region).append(S3BackupPath.PATH_SEP);
        buff.append(clusterName).append(S3BackupPath.PATH_SEP);
        
        token = factory.getInstance().getPayload();
        buff.append(token).append(S3BackupPath.PATH_SEP);
        // match the common characters to prefix.
        buff.append(match(start, end));
        return buff.toString();
    }
}
