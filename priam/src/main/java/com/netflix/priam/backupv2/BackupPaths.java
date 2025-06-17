package com.netflix.priam.backupv2;

import com.google.api.client.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.netflix.priam.compress.CompressionType;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public class BackupPaths {
    public static final String SEPARATOR = "/";
    public static final String PREFIX_SEPARATOR = "_";
    public static final Joiner JOINER = Joiner.on(SEPARATOR);
    private static final String CASS_PREFIX = "cass_";
    private static final int BACKUP_TYPE_INDEX = 3;
    private static final int LAST_MODIFIED_INDEX = 4;
    private static final Splitter SPLITTER = Splitter.on(SEPARATOR);

    public static String getSSTablePrefix(String path) {
        List<String> parts = Lists.newArrayList(SPLITTER.split(path));
        FileType fileType = FileType.valueOf(parts.get(BACKUP_TYPE_INDEX));
        String fileName;
        switch (fileType) {
            case SST_V2:
                fileName = parts.get(9);
                return fileName.substring(0, fileName.lastIndexOf("-"));
            case SECONDARY_INDEX_V2:
                fileName = parts.get(10);
                return fileName.substring(0, fileName.lastIndexOf("-"));
            case META_V2:
                return path;
            default:
                throw new IllegalArgumentException("Invalid file type: " + fileType + " in " + path);
        }
    }

    private static int getCompressionIndex(String backupPath) {
        List<String> parts = Lists.newArrayList(SPLITTER.split(backupPath));
        FileType fileType = FileType.valueOf(parts.get(BACKUP_TYPE_INDEX));
        if (fileType == FileType.SST_V2) {
            return 7;
        } else if (fileType == FileType.SECONDARY_INDEX_V2) {
            return 8;
        } else {
            throw new RuntimeException(String.format("Saw unknown file type %s", fileType));
        }
    }

    public static CompressionType getCompressionType(String backupPath) {
        int compressionIndex = getCompressionIndex(backupPath);
        return CompressionType.valueOf(Lists.newArrayList(SPLITTER.split(backupPath)).get(compressionIndex));
    }

    public static String removeCompressionPart(String backupPath) {
        List<String> parts = Lists.newArrayList(SPLITTER.split(backupPath));
        String compressionType = parts.remove(getCompressionIndex(backupPath));
        // checks compressionType validity
        CompressionType.valueOf(compressionType);
        return JOINER.join(parts);
    }

    public static Instant getLastModified(String backupPath) {
        String lastModified = SPLITTER.limit(LAST_MODIFIED_INDEX + 1).splitToList(backupPath).get(LAST_MODIFIED_INDEX);
        return Instant.ofEpochMilli(Long.parseLong(lastModified));
    }

    public static String getCommonPrefix() {
        return System.getenv("NETFLIX_ENVIRONMENT") + "_backup";
    }

    public static boolean isMetaFilePath(String path) {
        return path.matches("/META_V2/[\\d]+/[A-Z]+/[A-Z]+/meta_v2_\\d{12}\\.json$");
    }

    public static Optional<String> validateAppPrefix(String prefix) {
        String[] parts = prefix.split(PREFIX_SEPARATOR, 2);
        String msg = null;
        if (parts.length != 2) {
            msg = String.format("Can't split prefix %s on %s", prefix, PREFIX_SEPARATOR);

        }
        if (!parts[0].equals(parts[1].hashCode() % 10000)) {
            msg = String.format("%s is not the correct hash for %s", parts[0], parts[1]);
        }
        if (parts[1].startsWith(CASS_PREFIX)) {
            msg = String.format("%s does not start with %s", parts[1], CASS_PREFIX);
        }
        return Optional.ofNullable(msg);
    }

    public static Optional<String> validateTokenPrefix(String prefix) {
        List<String> parts = SPLITTER.splitToList(prefix);
        String token = parts.get(parts.size() - 1);
        String msg = null;
        try {
            new BigInteger(token);
        } catch (NumberFormatException e) {
            msg = String.format("Token %s in %s is not numeric.", token, prefix);
        }
        return Optional.ofNullable(msg);
    }
}
