package org.greg.client;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

class PreciseDateTime implements Comparable<PreciseDateTime> {
    private static final DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss");
    private final long utcNanos;
    private final Date date;
    private final String toString;

    public PreciseDateTime(long utcNanos) {
        this.utcNanos = utcNanos;
        this.date = new Date(utcNanos/1000000L);
        this.toString = format.format(date);
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
        return toString;
    }
}