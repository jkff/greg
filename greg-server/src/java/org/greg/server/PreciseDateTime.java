package org.greg.server;

import java.io.UnsupportedEncodingException;
import java.util.Calendar;

public class PreciseDateTime implements Comparable<PreciseDateTime> {
    private final long utcNanos;

    public PreciseDateTime(long utcNanos) {
        this.utcNanos = utcNanos;
    }

    public PreciseDateTime add(TimeSpan span) {
        return new PreciseDateTime(utcNanos + span.toNanos());
    }

    public int compareTo(PreciseDateTime o) {
        if(utcNanos < o.utcNanos)
            return -1;
        if(utcNanos > o.utcNanos)
            return 1;
        return 0;
    }

    public long toUtcNanos() {
        return utcNanos;
    }

    public String toString() {
        try {
            return new String(toBytes(), "us-ascii");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError();
        }
    }

    public byte[] toBytes() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(utcNanos/1000000L);

        int y = cal.get(Calendar.YEAR);
        int m = cal.get(Calendar.MONTH);
        int d = cal.get(Calendar.DAY_OF_MONTH);
        int hh = cal.get(Calendar.HOUR_OF_DAY);
        int mm = cal.get(Calendar.MINUTE);
        int ss = cal.get(Calendar.SECOND);
        int sss = cal.get(Calendar.MILLISECOND);

        // yyyy-mm-dd hh:mm:ss.sss
        // 01234567890123456789012
        byte[] c = new byte[23];
        c[3] = (byte) ('0' + (y%10)); y/=10;
        c[2] = (byte) ('0' + (y%10)); y/=10;
        c[1] = (byte) ('0' + (y%10)); y/=10;
        c[0] = (byte) ('0' + (y%10));

        c[4] = '-';

        c[6] = (byte) ('0' + (m%10)); m/=10;
        c[5] = (byte) ('0' + (m%10));

        c[7] = '-';

        c[9] = (byte) ('0' + (d%10)); d/=10;
        c[8] = (byte) ('0' + (d%10));

        c[10] = ' ';

        c[12] = (byte) ('0' + (hh%10)); hh/=10;
        c[11] = (byte) ('0' + (hh%10));

        c[13] = ':';
        c[15] = (byte) ('0' + (mm%10)); mm/=10;
        c[14] = (byte) ('0' + (mm%10));

        c[16] = ':';
        c[18] = (byte) ('0' + (ss%10)); ss/=10;
        c[17] = (byte) ('0' + (ss%10));

        c[19] = '.';
        c[22] = (byte) ('0' + (sss%10)); sss/=10;
        c[21] = (byte) ('0' + (sss%10)); sss/=10;
        c[20] = (byte) ('0' + (sss%10));

        return c;
    }
}
