package org.greg.server;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;

public class LittleEndianDataOutputStream implements DataOutput {
    private final OutputStream out;

    public LittleEndianDataOutputStream(OutputStream out) {
        this.out = out;
    }

    public void write(int b) throws IOException {
        out.write(b);
    }

    public void write(byte[] b) throws IOException {
        out.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    public void writeBoolean(boolean v) throws IOException {
        out.write(v ? 1 : 0);
    }

    public void writeByte(int v) throws IOException {
        out.write((byte)v);
    }

    public void writeShort(int v) throws IOException {
        out.write((byte)(v & 0xFF));
        out.write((byte)(v >>> 8));
    }

    public void writeChar(int v) throws IOException {
        out.write((byte)(v & 0xFF));
        out.write((byte)(v >>> 8));
    }

    public void writeInt(int v) throws IOException {
        out.write((byte)(v & 0xFF));
        out.write((byte)(v >>> 8));
        out.write((byte)(v >>> 16));
        out.write((byte)(v >>> 24));
    }

    public void writeLong(long v) throws IOException {
        out.write((byte)(v & 0xFF));
        out.write((byte)(v >>> 8));
        out.write((byte)(v >>> 16));
        out.write((byte)(v >>> 24));
        out.write((byte)(v >>> 32));
        out.write((byte)(v >>> 40));
        out.write((byte)(v >>> 48));
        out.write((byte)(v >>> 56));
    }

    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    public void writeBytes(String s) throws IOException {
        int len = s.length();
        for (int i = 0 ; i < len ; i++) {
            out.write((byte)s.charAt(i));
        }
    }

    public void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0 ; i < len ; i++) {
            int v = s.charAt(i);
            out.write((v >>> 8) & 0xFF);
            out.write(v & 0xFF);
        }
    }

    public void writeUTF(String str) throws IOException {
        // Copied from DataOutputStream
        int strlen = str.length();
        int utflen = 0;
        int c;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535)
            throw new UTFDataFormatException(
                "encoded string too long: " + utflen + " bytes");

        byte[] bytearr = new byte[(utflen*2) + 2];

        int count = 0;
        bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
        bytearr[count++] = (byte) (utflen & 0xFF);

        int i=0;
        for (i=0; i<strlen; i++) {
           c = str.charAt(i);
           if (!((c >= 0x0001) && (c <= 0x007F))) break;
           bytearr[count++] = (byte) c;
        }

        for (;i < strlen; i++){
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytearr[count++] = (byte) c;

            } else if (c > 0x07FF) {
                bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytearr[count++] = (byte) (0x80 | ((c >>  6) & 0x3F));
                bytearr[count++] = (byte) (0x80 | (c & 0x3F));
            } else {
                bytearr[count++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
                bytearr[count++] = (byte) (0x80 | (c & 0x3F));
            }
        }
        out.write(bytearr, 0, utflen+2);
    }
}
