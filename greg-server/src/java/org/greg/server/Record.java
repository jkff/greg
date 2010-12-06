package org.greg.server;

public class Record {
    public ByteSlice machine;
    public PreciseDateTime serverTimestamp;
    public PreciseDateTime timestamp;
    public ByteSlice message;
    public ByteSlice clientId;
}
