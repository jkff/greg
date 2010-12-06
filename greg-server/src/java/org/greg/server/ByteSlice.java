package org.greg.server;

public class ByteSlice {
    byte[] array;
    int offset;
    int len;

    public ByteSlice(byte[] array, int offset, int len) {
        this.array = array;
        this.offset = offset;
        this.len = len;
    }
}
