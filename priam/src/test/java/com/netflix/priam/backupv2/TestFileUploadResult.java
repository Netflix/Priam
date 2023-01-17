package com.netflix.priam.backupv2;

import com.google.common.truth.Truth;
import java.nio.file.Paths;
import java.time.Instant;
import org.junit.Test;

/** unit tests of FileUploadResult */
public class TestFileUploadResult {

    @Test
    public void standardInput() {
        Truth.assertThat(getResult().toString())
                .isEqualTo(
                        "{\n"
                                + "  \"fileName\": \"path\",\n"
                                + "  \"lastModifiedTime\": 200000,\n"
                                + "  \"fileCreationTime\": 100000,\n"
                                + "  \"fileSizeOnDisk\": 100000,\n"
                                + "  \"compression\": \"SNAPPY\",\n"
                                + "  \"encryption\": \"PLAINTEXT\"\n"
                                + "}");
    }

    @Test
    public void setIsUploaded() {
        FileUploadResult result = getResult();
        result.setUploaded(true);
        Truth.assertThat(result.toString())
                .isEqualTo(
                        "{\n"
                                + "  \"fileName\": \"path\",\n"
                                + "  \"lastModifiedTime\": 200000,\n"
                                + "  \"fileCreationTime\": 100000,\n"
                                + "  \"fileSizeOnDisk\": 100000,\n"
                                + "  \"compression\": \"SNAPPY\",\n"
                                + "  \"encryption\": \"PLAINTEXT\",\n"
                                + "  \"isUploaded\": true\n"
                                + "}");
    }

    @Test
    public void setBackupPath() {
        FileUploadResult result = getResult();
        result.setBackupPath("foo");
        Truth.assertThat(result.toString())
                .isEqualTo(
                        "{\n"
                                + "  \"fileName\": \"path\",\n"
                                + "  \"lastModifiedTime\": 200000,\n"
                                + "  \"fileCreationTime\": 100000,\n"
                                + "  \"fileSizeOnDisk\": 100000,\n"
                                + "  \"compression\": \"SNAPPY\",\n"
                                + "  \"encryption\": \"PLAINTEXT\",\n"
                                + "  \"backupPath\": \"foo\"\n"
                                + "}");
    }

    @Test
    public void getBackupPath() {
        FileUploadResult result = getResult();
        result.setBackupPath("foo");
        Truth.assertThat(result.getBackupPath()).isEqualTo("foo");
    }

    private FileUploadResult getResult() {
        return new FileUploadResult(
                Paths.get("/my/file/path"),
                Instant.ofEpochMilli(200000L),
                Instant.ofEpochMilli(100000L),
                100000L);
    }
}
