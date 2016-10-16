package com.netflix.priam.merics;

/**
 *
 * Represents a specific measurement for publishing to a metric system
 *
 * Created by vinhn on 10/14/16.
 */
public interface IMeasurement {

    public MMEASUREMENT_TYPE getType();
    public void incrementFailureCnt(int i);
    public int getFailureCnt();
    public void incrementSuccessCnt(int i);
    public int getSuccessCnt();

    public enum MMEASUREMENT_TYPE {
        NOOP, NODETOOLFLUSH, SNAPSHOTBACKUP;
    };
}
