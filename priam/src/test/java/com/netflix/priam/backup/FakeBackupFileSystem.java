package com.netflix.priam.backup;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.netflix.priam.aws.S3BackupPath;
import com.netflix.priam.backup.AbstractBackupPath;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;

@Singleton
public class FakeBackupFileSystem implements IBackupFileSystem
{
    private List<AbstractBackupPath> flist;
    public Set<String> downloadedFiles;
    public Set<String> uploadedFiles;
    public String baseDir, region, clusterName;

    @Inject
    Provider<S3BackupPath> pathProvider;

    public void setupTest(List<String> files)
    {
        clearTest();
        flist = new ArrayList<AbstractBackupPath>();
        for (String file : files)
        {
            S3BackupPath path = pathProvider.get();
            path.parseRemote(file);
            flist.add(path);
        }
        downloadedFiles = new HashSet<String>();
        uploadedFiles = new HashSet<String>();
    }

    public void setupTest()
    {
        clearTest();
        flist = new ArrayList<AbstractBackupPath>();
        downloadedFiles = new HashSet<String>();
        uploadedFiles = new HashSet<String>();
    }

    public void clearTest()
    {
        if (flist != null)
            flist.clear();
        if (downloadedFiles != null)
            downloadedFiles.clear();
    }

    public void addFile(String file)
    {
        S3BackupPath path = pathProvider.get();
        path.parseRemote(file);
        flist.add(path);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void download(AbstractBackupPath path, OutputStream os) throws BackupRestoreException
    {
        try
        {
            if (path.type == BackupFileType.META)
            {
                // List all files and generate the file
                FileWriter fr = new FileWriter(path.newRestoreFile());
                try
                {
                    JSONArray jsonObj = new JSONArray();
                    for (AbstractBackupPath filePath : flist)
                    {
                        if (filePath.type == BackupFileType.SNAP)
                            jsonObj.add(filePath.getRemotePath());
                    }
                    fr.write(jsonObj.toJSONString());
                }
                finally
                {
                    IOUtils.closeQuietly(fr);
                }
            }
            downloadedFiles.add(path.getRemotePath());
            System.out.println("Downloading " + path.getRemotePath());
        }
        catch (IOException io)
        {
            throw new BackupRestoreException(io.getMessage(), io);
        }
    }

    @Override
    public void upload(AbstractBackupPath path, InputStream in) throws BackupRestoreException
    {
        uploadedFiles.add(path.backupFile.getAbsolutePath());
    }

    @Override
    public Iterator<AbstractBackupPath> list(String bucket, Date start, Date till)
    {
        String[] paths = bucket.split(String.valueOf(S3BackupPath.PATH_SEP));
        
        if( paths.length > 1){
            baseDir = paths[1];
            region = paths[2];
            clusterName = paths[3];
        }
        
        List<AbstractBackupPath> tmpList = new ArrayList<AbstractBackupPath>();
        for (AbstractBackupPath path : flist)
        {

            if ((path.time.after(start) && path.time.before(till)) || path.time.equals(start)
                && path.baseDir.equals(baseDir) && path.clusterName.equals(clusterName) && path.region.equals(region))
            {
                 tmpList.add(path);
            }
        }
        return tmpList.iterator();
    }

    @Override
    public int getActivecount()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    public void shutdown()
    {
        //nop
    }

    @Override
    public long getBytesUploaded() {
        return 0;
    }

    @Override
    public Iterator<AbstractBackupPath> listPrefixes(Date date)
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void cleanup()
    {
        // TODO Auto-generated method stub
        
    }

	@Override
	public void download(AbstractBackupPath path, OutputStream os,
			String diskPath) throws BackupRestoreException {

        try
        {
            if (path.type == BackupFileType.META)
            {
                // List all files and generate the file
                FileWriter fr = new FileWriter(path.newRestoreFile());
                try
                {
                    JSONArray jsonObj = new JSONArray();
                    for (AbstractBackupPath filePath : flist)
                    {
                        if (filePath.type == BackupFileType.SNAP)
                            jsonObj.add(filePath.getRemotePath());
                    }
                    fr.write(jsonObj.toJSONString());
                }
                finally
                {
                    IOUtils.closeQuietly(fr);
                }
            }
            downloadedFiles.add(path.getRemotePath());
            System.out.println("Downloading " + path.getRemotePath());
        }
        catch (IOException io)
        {
            throw new BackupRestoreException(io.getMessage(), io);
        }
		
	}

}
