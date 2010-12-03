package org.greg.client;

public class Configuration {
    public final String server = System.getProperty("greg.server", "localhost");
    public final int port = Integer.parseInt(System.getProperty("greg.port", "5676"));
    public final int calibrationPort = Integer.parseInt(System.getProperty("greg.calibrationPort", "5677"));
    public final int flushPeriodMs = Integer.parseInt(System.getProperty("greg.flushPeriodMs", "1000"));
    public final String clientId = System.getProperty("greg.clientId", "unknown");
    public final int maxBufferedRecords = Integer.parseInt(System.getProperty("greg.maxBufferedRecords", "100000"));
    public final boolean useCompression = Boolean.parseBoolean(System.getProperty("greg.useCompression", "true"));
    public final int calibrationPeriodSec = Integer.parseInt(System.getProperty("greg.calibrationPeriodSec", "10"));

    /**
     * This field is not final - you can change it if you wish to use
     * your own configuration mechanism.
     */
    public static Configuration INSTANCE = new Configuration();
}
