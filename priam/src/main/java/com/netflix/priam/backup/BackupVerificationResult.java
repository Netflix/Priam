package com.netflix.priam.backup;

import java.util.List;

/**
 * Created by aagrawal on 2/16/17.
 */

public class BackupVerificationResult
{
    public boolean snapshotAvailable = false;
    public boolean valid = false;
    public boolean metaFileFound = false;
    public String selectedDate = null;
    public String snapshotTime = null;
    public List<String> filesInMetaOnly = null;
    public List<String> filesInS3Only = null;
    public List<String> filesMatched = null;
}