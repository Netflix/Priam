package com.netflix.priam.merics;

/**
 *
 * Represents a specific measurement for publishing to a metric system
 *
 * Created by vinhn on 10/14/16.
 */
public interface IMeasurement<T> {

    public MMEASUREMENT_TYPE getType();
    public void incrementFailureCnt(int i);
    public int getFailureCnt();
    public void incrementSuccessCnt(int i);
    public int getSuccessCnt();
    /*
    @return a user defined representation of a valuue.
     */
    public T getVal();
    /*
    @param a user defined representation of what you think is a value.
     */
    public void setVal(T val);

    public enum MMEASUREMENT_TYPE {
        NOOP, NODETOOLFLUSH, SNAPSHOTBACKUP, BACKUPUPLOADRATE
        , SNAPSHOTBACKUPUPNOTIFICATION
        , AWSSLOWDOWNEXCEPTION
        ;
    };
}
