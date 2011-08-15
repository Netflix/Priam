package com.priam.backup;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.priam.backup.AbstractBackupPath.BackupFileType;
import com.priam.identity.InstanceIdentity;

public class MetaData
{
    private static final Logger logger = LoggerFactory.getLogger(MetaData.class);
    private Provider<AbstractBackupPath> pathProvider;
    private IBackupFileSystem fs;

    @Inject
    InstanceIdentity id;

    @Inject
    public MetaData(Provider<AbstractBackupPath> pathProvider, IBackupFileSystem fs)
    {
        this.pathProvider = pathProvider;
        this.fs = fs;
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
            metafile.delete();
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
