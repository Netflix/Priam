package com.netflix.priam.backup;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.identity.InstanceIdentity;
import com.netflix.priam.utils.RetryableCallable;

/**
 * Class to create a meta data file with a list of snapshot files. Also list the
 * contents of a meta data file.
 */
public class MetaData
{
    private static final Logger logger = LoggerFactory.getLogger(MetaData.class);
    private final Provider<AbstractBackupPath> pathFactory;
    private final IBackupFileSystem fs;

    @Inject
    public MetaData(IBackupFileSystem fs, Provider<AbstractBackupPath> pathFactory)
    {
        this.pathFactory = pathFactory;
        this.fs = fs;
    }

    @SuppressWarnings("unchecked")
    public void set(List<AbstractBackupPath> bps, String snapshotName) throws Exception
    {
        File metafile = new File("/tmp/meta.json");
        FileWriter fr = new FileWriter(metafile);
        try
        {
            JSONArray jsonObj = new JSONArray();
            for (AbstractBackupPath filePath : bps)
                jsonObj.add(filePath.getRemotePath());
            fr.write(jsonObj.toJSONString());
        }
        finally
        {
            IOUtils.closeQuietly(fr);
        }
        AbstractBackupPath backupfile = pathFactory.get();
        backupfile.parseLocal(metafile, BackupFileType.META);
        backupfile.time = backupfile.getFormat().parse(snapshotName);
        try
        {
            upload(backupfile);
        }
        finally
        {
            FileUtils.deleteQuietly(metafile);
        }
    }

    public List<AbstractBackupPath> get(final AbstractBackupPath meta)
    {
        List<AbstractBackupPath> files = Lists.newArrayList();
        try
        {
            new RetryableCallable<Void>()
            {
                @Override
                public Void retriableCall() throws Exception
                {
                    fs.download(meta, new FileOutputStream(meta.newRestoreFile()));
                    return null;
                }
            }.call();

            File file = meta.newRestoreFile();
            JSONArray jsonObj = (JSONArray) new JSONParser().parse(new FileReader(file));
            for (int i = 0; i < jsonObj.size(); i++)
            {
                AbstractBackupPath p = pathFactory.get();
                p.parseRemote((String) jsonObj.get(i));
                files.add(p);
            }
        }
        catch (Exception ex)
        {
            logger.error("Error downloading the Meta data try with a diffrent date...", ex);
        }
        return files;
    }

    private void upload(final AbstractBackupPath bp) throws Exception
    {
        new RetryableCallable<Void>()
        {
            @Override
            public Void retriableCall() throws Exception
            {
                fs.upload(bp, bp.localReader());
                return null;
            }
        }.call();
    }

    public void download(final AbstractBackupPath path) throws Exception
    {
        new RetryableCallable<Void>()
        {
            @Override
            public Void retriableCall() throws Exception
            {
                fs.download(path, new FileOutputStream(path.newRestoreFile()));
                return null;
            }
        }.call();
    }
}
