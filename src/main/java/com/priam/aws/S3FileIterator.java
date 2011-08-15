package com.priam.aws;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.Lists;
import com.google.inject.Provider;
import com.priam.backup.AbstractBackupPath;

public class S3FileIterator implements Iterator<AbstractBackupPath>
{
    private Provider<AbstractBackupPath> pathProvider;
    private Iterator<AbstractBackupPath> iterator;
    private ObjectListing objectListing;
    private AmazonS3 s3Client;
    private Date start;
    private Date till;

    public S3FileIterator(Provider<AbstractBackupPath> pathProvider, AmazonS3 s3Client, String bucket, Date start, Date till)
    {
        this.start = start;
        this.till = till;
        this.pathProvider = pathProvider;
        ListObjectsRequest listReq = new ListObjectsRequest();
        listReq.setBucketName(bucket);
        listReq.setPrefix(pathProvider.get().remotePrefix(start, till));
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
            if (objectListing.isTruncated() || objectListing.getObjectSummaries().size() > 0)
            {
                return false;
            }
            else
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
            if (path.time.after(start) && path.time.before(till))
                temp.add(path);
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
