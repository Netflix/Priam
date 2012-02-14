package com.netflix.priam.backup;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
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

/**
 * Class to create a meta data file with a list of snapshot files. Also list the
 * contents of a meta data file.
 */
public class MetaData
{    
    protected final Provider<AbstractBackupPath> pathProvider;
    protected final IBackupFileSystem fs;
    protected final InstanceIdentity id;

    private static final Logger logger = LoggerFactory.getLogger(MetaData.class);
    
    @Inject
    public MetaData(Provider<AbstractBackupPath> pathProvider, IBackupFileSystem fs, InstanceIdentity id)
    {
        this.pathProvider = pathProvider;
        this.fs = fs;
        this.id = id;
    }

    @SuppressWarnings("unchecked")
    public void set(List<AbstractBackupPath> bps, String snapshotName) throws IOException, ParseException, BackupRestoreException
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
        AbstractBackupPath backupfile = pathProvider.get();
        backupfile.parseLocal(metafile, BackupFileType.META);
        backupfile.time = backupfile.getFormat().parse(snapshotName);
        try
        {
            fs.upload(backupfile);
        }
        finally
        {
            FileUtils.deleteQuietly(metafile);
        }
    }

    public List<AbstractBackupPath> get(AbstractBackupPath meta)
    {
        List<AbstractBackupPath> files = Lists.newArrayList();
        try
        {
            fs.download(meta);
            File file = meta.newRestoreFile();
            JSONArray jsonObj = (JSONArray) new JSONParser().parse(new FileReader(file));
            for (int i = 0; i < jsonObj.size(); i++)
            {
                AbstractBackupPath p = pathProvider.get();
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
}
