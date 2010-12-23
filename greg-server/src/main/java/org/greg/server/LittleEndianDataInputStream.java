package org.greg.server;

import java.io.*;

public class LittleEndianDataInputStream implements DataInput {
    private InputStream in;

    public LittleEndianDataInputStream(InputStream in) {
        this.in = in;
    }

    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    public void readFully(byte[] b, int off, int len) throws IOException {
        if (len < 0)
            throw new IndexOutOfBoundsException();
        int n = 0;
        while (n < len) {
            int count = in.read(b, off + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        }
    }

    public int skipBytes(int n) throws IOException {
        int total = 0;
        int cur = 0;

        while ((total < n) && ((cur = (int) in.skip(n - total)) > 0)) {
            total += cur;
        }

        return total;
    }

    public boolean readBoolean() throws IOException {
        int b = in.read();
        if (b < 0)
            throw new EOFException();
        return (b != 0);
    }

    public byte readByte() throws IOException {
        int b = in.read();
        if (b < 0)
            throw new EOFException();
        return (byte) b;
    }

    public int readUnsignedByte() throws IOException {
        int b = in.read();
        if (b < 0)
            throw new EOFException();
        return (byte) b;
    }

    public short readShort() throws IOException {
        int a = in.read();
        int b = in.read();
        if ((a | b) < 0)
            throw new EOFException();
        return (short) ((b << 8) + a);
    }

    public int readUnsignedShort() throws IOException {
        int a = in.read();
        int b = in.read();
        if ((a | b) < 0)
            throw new EOFException();
        return ((b << 8) + a);
    }

    public char readChar() throws IOException {
        int ch1 = in.read();
        int ch2 = in.read();
        if ((ch1 | ch2) < 0)
            throw new EOFException();
        return (char) ((ch2 << 8) + ch1);
    }

    public int readInt() throws IOException {
        int a = in.read();
        int b = in.read();
        int c = in.read();
        int d = in.read();
        if ((a | b | c | d) < 0)
            throw new EOFException();
        return ((d << 24) + (c << 16) + (b << 8) + a);
    }

    public long readLong() throws IOException {
        long a = in.read();
        long b = in.read();
        long c = in.read();
        long d = in.read();
        long e = in.read();
        long f = in.read();
        long g = in.read();
        long h = in.read();
        if ((a | b | c | d | e | f | g | h) < 0)
            throw new EOFException();
        return ((h << 56) + (g << 48) + (f << 40) + (e << 32) + (d << 24) + (c << 16) + (b << 8) + a);
    }

    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public String readLine() throws IOException {
        // Copied from DataInputStream
        char lineBuffer[];
        char buf[];
        buf = lineBuffer = new char[128];
        int room = buf.length;
        int offset = 0;
        int c;

        loop:
        while (true) {
            switch (c = in.read()) {
                case -1:
                case '\n':
                    break loop;

                case '\r':
                    int c2 = in.read();
                    if ((c2 != '\n') && (c2 != -1)) {
                        if (!(in instanceof PushbackInputStream)) {
                            this.in = new PushbackInputStream(in);
                        }
                        ((PushbackInputStream) in).unread(c2);
                    }
                    break loop;

                default:
                    if (--room < 0) {
                        buf = new char[offset + 128];
                        room = buf.length - offset - 1;
                        System.arraycopy(lineBuffer, 0, buf, 0, offset);
                        lineBuffer = buf;
                    }
                    buf[offset++] = (char) c;
                    break;
            }
        }
        if ((c == -1) && (offset == 0)) {
            return null;
        }
        return String.copyValueOf(buf, 0, offset);
    }

    public String readUTF() throws IOException {
        return DataInputStream.readUTF(this);
    }
}
