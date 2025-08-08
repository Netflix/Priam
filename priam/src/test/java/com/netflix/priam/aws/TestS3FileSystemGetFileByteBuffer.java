/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.priam.aws;

import com.google.api.client.util.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.priam.backup.AbstractBackupPath.BackupFileType;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backup.BackupRestoreException;
import com.netflix.priam.compress.CompressionType;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestS3FileSystemGetFileByteBuffer {
    private static Injector injector;
    private static final File DIR = new File("target/data/KS1/CF1/backups/201108082320/");

    public TestS3FileSystemGetFileByteBuffer() {
        if (injector == null) injector = Guice.createInjector(new BRTestModule());
    }

    @BeforeClass
    public static void setUp() {
        if (!DIR.exists()) DIR.mkdirs();
    }

    @AfterClass
    public static void cleanup() throws IOException {
        FileUtils.cleanDirectory(DIR);
    }

    @Test
    public void testGetFileByteBufferNoCompression() throws Exception {
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        RemoteBackupPath backupPath = injector.getInstance(RemoteBackupPath.class);
        File testFile = localFile();
        backupPath.parseLocal(testFile, BackupFileType.SST_V2);
        backupPath.setCompression(CompressionType.NONE);
        
        ByteBuffer result = fs.getFileByteBuffer(backupPath);
        
        Assert.assertNotNull(result);
        Assert.assertEquals(testFile.length(), result.remaining());
        
        byte[] expected = new byte[5 << 10];
        Arrays.fill(expected, (byte) 8);
        byte[] actual = new byte[result.remaining()];
        result.get(actual);
        Assert.assertArrayEquals(expected, actual);
    }

    @Test
    public void testGetFileByteBufferWithSnappyCompression() throws Exception {
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        RemoteBackupPath backupPath = injector.getInstance(RemoteBackupPath.class);
        File testFile = localFile();
        backupPath.parseLocal(testFile, BackupFileType.SST_V2);
        backupPath.setCompression(CompressionType.SNAPPY);
        
        ByteBuffer result = fs.getFileByteBuffer(backupPath);
        
        Assert.assertNotNull(result);
        Assert.assertTrue(result.remaining() > 0);
        Assert.assertTrue(result.remaining() < testFile.length()); // Should be compressed
    }

    @Test
    public void testGetFileByteBufferEmptyFile() throws Exception {
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        RemoteBackupPath backupPath = injector.getInstance(RemoteBackupPath.class);
        
        String caller = Thread.currentThread().getStackTrace()[1].getMethodName();
        File emptyFile = new File(DIR + caller + "empty-file.db");
        emptyFile.createNewFile();
        
        backupPath.parseLocal(emptyFile, BackupFileType.SST_V2);
        backupPath.setCompression(CompressionType.NONE);
        
        ByteBuffer result = fs.getFileByteBuffer(backupPath);
        
        Assert.assertNotNull(result);
        Assert.assertEquals(0, result.remaining());
    }

    @Test(expected = BackupRestoreException.class)
    public void testGetFileByteBufferNonExistentFile() throws Exception {
        S3FileSystem fs = injector.getInstance(S3FileSystem.class);
        RemoteBackupPath backupPath = injector.getInstance(RemoteBackupPath.class);
        
        File nonExistentFile = new File(DIR + "non-existent-file.db");
        backupPath.parseLocal(nonExistentFile, BackupFileType.SST_V2);
        
        fs.getFileByteBuffer(backupPath);
    }

    private File localFile() throws IOException {
        String caller = Thread.currentThread().getStackTrace()[1].getMethodName();
        File file = new File(DIR + caller + "KS1-CF1-ia-1-Data.db");
        if (file.createNewFile()) {
            byte[] data = new byte[5 << 10];
            Arrays.fill(data, (byte) 8);
            try (BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(file))) {
                os.write(data);
            }
        }
        Preconditions.checkState(file.exists());
        return file;
    }
}