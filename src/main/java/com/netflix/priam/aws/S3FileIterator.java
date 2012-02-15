package com.netflix.priam.aws;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath;

/**
 * Iterator representing list of backup files available on S3
 */
public class S3FileIterator implements Iterator<AbstractBackupPath>
{    
    protected final Provider<AbstractBackupPath> pathProvider;
    protected final AmazonS3 s3Client;
    protected Iterator<AbstractBackupPath> iterator;
    protected ObjectListing objectListing;    
    protected Date start;
    protected Date till;
    private static final Logger logger = LoggerFactory.getLogger(S3FileIterator.class);

    public S3FileIterator(Provider<AbstractBackupPath> pathProvider, AmazonS3 s3Client, String path, Date start, Date till)
    {
        this.start = start;
        this.till = till;
        this.pathProvider = pathProvider;
        ListObjectsRequest listReq = new ListObjectsRequest();
        String[] paths = path.split(String.valueOf(S3BackupPath.PATH_SEP));        
        listReq.setBucketName(paths[0]);
        listReq.setPrefix(pathProvider.get().remotePrefix(start, till, path));
        this.s3Client = s3Client;
        objectListing = s3Client.listObjects(listReq);
        iterator = createIterator();
    }

    @Override
    public boolean hasNext()
    {
        if (iterator.hasNext())
        {
            return true;
        }
        else
        {
            while(objectListing.isTruncated() && !iterator.hasNext()) 
            {
                objectListing = s3Client.listNextBatchOfObjects(objectListing);                
                iterator = createIterator();
            }

        }
        return iterator.hasNext();
    }

    private Iterator<AbstractBackupPath> createIterator()
    {
        List<AbstractBackupPath> temp = Lists.newArrayList();
        for (S3ObjectSummary summary : objectListing.getObjectSummaries())
        {
            AbstractBackupPath path = pathProvider.get();
            path.parseRemote(summary.getKey());
            logger.debug("New key " + summary.getKey() + " path = " + path.getRemotePath() + " " + start + " end: " + till + " my " + path.getTime() );
            if ((path.getTime().after(start) && path.getTime().before(till)) || path.getTime().equals(start)){
                temp.add(path);
                logger.debug("Added key " + summary.getKey() );
            }
        }
        return temp.iterator();
    }

    @Override
    public AbstractBackupPath next()
    {
        return iterator.next();
    }

    @Override
    public void remove()
    {
        throw new IllegalStateException();
    }
}
