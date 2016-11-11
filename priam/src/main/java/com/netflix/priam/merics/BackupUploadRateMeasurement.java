package com.netflix.priam.merics;

/**
 * Created by vinhn on 10/19/16.
 */
public class BackupUploadRateMeasurement implements IMeasurement<BackupUploadRateMeasurement.Metadata> {

    private BackupUploadRateMeasurement.Metadata metadata;
    private int incrementSuccessCnt = 0;

    @Override
    public MMEASUREMENT_TYPE getType() {
        return IMeasurement.MMEASUREMENT_TYPE.BACKUPUPLOADRATE;
    }

    @Override
    public void incrementFailureCnt(int i) {

        throw new UnsupportedOperationException();
    }

    @Override
    public int getFailureCnt() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void incrementSuccessCnt(int i) {
        this.incrementSuccessCnt += i;
    }

    @Override
    public int getSuccessCnt() {
        return this.incrementSuccessCnt;
    }

    @Override
    public BackupUploadRateMeasurement.Metadata getVal() {
        return this.metadata;
    }

    @Override
    public void setVal(BackupUploadRateMeasurement.Metadata val) {
        this.metadata = val;
    }

    public static class Metadata {
        private final double uploadRateKBps;
        private final long elapseTimeInMillisecs;
        private final String fileName;

        public Metadata(String fileName, double uploadRateKBps, long elapseTimeInMillisecs) {
            this.fileName = fileName;
            this.uploadRateKBps = uploadRateKBps;
            this.elapseTimeInMillisecs = elapseTimeInMillisecs;
        }
        public String getFileName() {
            return this.fileName;
        }
        public double getUploadRateKBps() {
            return this.uploadRateKBps;
        }
        public long getGetElapseTimeInMillisecs() {return this.elapseTimeInMillisecs; }
    }
}
