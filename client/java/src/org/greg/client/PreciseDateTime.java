package org.greg.client;

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

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PreciseDateTime that = (PreciseDateTime) o;

        if (utcNanos != that.utcNanos) return false;

        return true;
    }

    public int hashCode() {
        return (int) (utcNanos ^ (utcNanos >>> 32));
    }

    public long toUtcNanos() {
        return utcNanos;
    }

    public String toString() {
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
        char[] c = new char[23];
        c[3] = (char) ('0' + (y%10)); y/=10;
        c[2] = (char) ('0' + (y%10)); y/=10;
        c[1] = (char) ('0' + (y%10)); y/=10;
        c[0] = (char) ('0' + (y%10));

        c[4] = '-';

        c[6] = (char) ('0' + (m%10)); m/=10;
        c[5] = (char) ('0' + (m%10));

        c[7] = '-';

        c[9] = (char) ('0' + (d%10)); d/=10;
        c[8] = (char) ('0' + (d%10));

        c[10] = ' ';

        c[12] = (char) ('0' + (hh%10)); hh/=10;
        c[11] = (char) ('0' + (hh%10));

        c[13] = ':';
        c[15] = (char) ('0' + (mm%10)); mm/=10;
        c[14] = (char) ('0' + (mm%10));

        c[16] = ':';
        c[18] = (char) ('0' + (ss%10)); ss/=10;
        c[17] = (char) ('0' + (ss%10));

        c[19] = '.';
        c[22] = (char) ('0' + (sss%10)); sss/=10;
        c[21] = (char) ('0' + (sss%10)); sss/=10;
        c[20] = (char) ('0' + (sss%10));

        return new String(c);
    }
}
