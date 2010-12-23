package org.greg.client;

class TimeSpan {
    private long nanos;

    public TimeSpan(long nanos) {
        this.nanos = nanos;
    }

    public double toSeconds() {
        return (1.0 * nanos) / 1.0e9;
    }

    public double toMillis() {
        return (1.0 * nanos) / 1.0e6;
    }

    public long toNanos() {
        return nanos;
    }

    public String toString() {
        return (1.0e-6 * nanos) + "ms";
    }
}
