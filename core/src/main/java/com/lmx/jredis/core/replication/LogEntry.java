package com.lmx.jredis.core.replication;

/**
 * //TODO base on offset to replication
 *
 * @author : lucas
 * @date 2019/07/29
 */
public class LogEntry {
    private long startOffset;
    private int logDbIdx;
    private String logType;
    private byte[] logData;

    public LogEntry(long startOffset, int logDbIdx, String logType, byte[] logData) {
        this.startOffset = startOffset;
        this.logDbIdx = logDbIdx;
        this.logType = logType;
        this.logData = logData;
    }
}
