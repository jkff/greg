package org.greg.server;

public class PreciseClock implements Clock {
    private static final long utcOrigin = System.currentTimeMillis();
    private static final long originNano = System.nanoTime();

    public static final PreciseClock INSTANCE = new PreciseClock();

    public PreciseDateTime now() {
        return new PreciseDateTime(1000000L * utcOrigin + (System.nanoTime() - originNano));
    }
}
