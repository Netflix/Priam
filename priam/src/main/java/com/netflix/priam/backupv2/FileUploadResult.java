/*
 * Copyright 2018 Netflix, Inc.
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
package com.netflix.priam.backupv2;

import com.netflix.priam.compress.ICompression;
import com.netflix.priam.utils.GsonJsonSerializer;
import org.codehaus.jettison.json.JSONObject;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;

/**
 * This is a POJO that will encapsulate the result of file upload.
 * Created by aagrawal on 6/20/18.
 */
public class FileUploadResult {
    private Path fileName;
    @GsonJsonSerializer.PriamAnnotation.GsonIgnore
    private String keyspaceName;
    @GsonJsonSerializer.PriamAnnotation.GsonIgnore
    private String columnFamilyName;
    private Instant lastModifiedTime;
    private Instant fileCreationTime;
    private long fileSizeOnDisk; //Size on disk in bytes
    private Boolean isUploaded;
    //Valid compression technique for now is SNAPPY only. Future we need to support LZ4 and NONE
    private ICompression.CompressionAlgorithm compression = ICompression.CompressionAlgorithm.SNAPPY;
    private Path backupPath;

    public FileUploadResult(Path fileName, String keyspaceName, String columnFamilyName, Instant lastModifiedTime, Instant fileCreationTime, long fileSizeOnDisk) {
        this.fileName = fileName;
        this.keyspaceName = keyspaceName;
        this.columnFamilyName = columnFamilyName;
        this.lastModifiedTime = lastModifiedTime;
        this.fileCreationTime = fileCreationTime;
        this.fileSizeOnDisk = fileSizeOnDisk;
    }

    public static FileUploadResult getFileUploadResult(String keyspaceName, String columnFamilyName, Path file) throws Exception {
        BasicFileAttributes fileAttributes = Files.readAttributes(file, BasicFileAttributes.class);
        return new FileUploadResult(file, keyspaceName, columnFamilyName, fileAttributes.lastModifiedTime().toInstant(), fileAttributes.creationTime().toInstant(), fileAttributes.size());
    }

    public static FileUploadResult getFileUploadResult(String keyspaceName, String columnFamilyName, File file) throws Exception {
        return getFileUploadResult(keyspaceName, columnFamilyName, file.toPath());
    }

    public Path getFileName() {
        return fileName;
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public String getColumnFamilyName() {
        return columnFamilyName;
    }

    public Instant getLastModifiedTime() {
        return lastModifiedTime;
    }

    public Instant getFileCreationTime() {
        return fileCreationTime;
    }

    public long getFileSizeOnDisk() {
        return fileSizeOnDisk;
    }

    public Boolean getUploaded() {
        return isUploaded;
    }

    public void setUploaded(Boolean uploaded) {
        isUploaded = uploaded;
    }

    public ICompression.CompressionAlgorithm getCompression() {
        return compression;
    }

    public void setCompression(ICompression.CompressionAlgorithm compression) {
        this.compression = compression;
    }

    public Path getBackupPath() {
        return backupPath;
    }

    public void setBackupPath(Path backupPath) {
        this.backupPath = backupPath;
    }

    //
    public JSONObject getJSONObject() throws Exception {
        JSONObject result = new JSONObject();
        result.put("file", fileName.toFile().getName());
        result.put("modify", lastModifiedTime.toEpochMilli());
        result.put("creation", fileCreationTime.toEpochMilli());
        result.put("size", fileSizeOnDisk);
        result.put("compression", compression.name());
        result.put("uploaded", isUploaded);
        result.put("loc", backupPath);
        return result;
    }

    @Override
    public String toString() {
        return GsonJsonSerializer.getGson().toJson(this);
    }
}
