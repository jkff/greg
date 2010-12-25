package org.greg.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.Properties;

public class Configuration {
    public static final String SERVER = "greg.server";
    public static final String HOST_NAME = "greg.hostname";
    public static final String PORT = "greg.port";
    public static final String CALIBRATION_PORT = "greg.port";
    public static final String CALIBRATION_PERIOD_SEC = "greg.calibrationPeriodSec";
    public static final String FLUSH_PERIOD_MS = "greg.flushPeriodMs";
    public static final String CLIENT_ID = "greg.clientId";
    public static final String MAX_BUFFERED_RECORDS = "greg.maxBufferedRecords";
    public static final String USE_COMPRESSION = "greg.useCompression";

    /**
     * This field is not final - you can change it if you wish to use
     * your own configuration mechanism.
     */
    public static final Configuration INSTANCE = new Configuration();

    private static final String defaultPropertiesPath = "/greg.properties";
    private Properties properties = new Properties();
    private String server;
    private String hostname;
    private int port;
    private int calibrationPort;
    private int calibrationPeriodSec;
    private int flushPeriodMs;
    private String clientId;
    private int maxBufferedRecords;
    private boolean useCompression;

    public Configuration() {
        this(defaultPropertiesPath);
    }

    public Configuration(String path) {
        this(Configuration.class.getClass().getResourceAsStream(path));
    }

    public Configuration(InputStream is) {
        Properties defaultProperties = new Properties();
        if (is != null) {
            try {
                load(is, defaultProperties);
            } catch (IOException e) {
                // ignore
            } finally {
                close(is);
            }
        }
    }

    public String get(String key) {
        return properties.getProperty(key);
    }

    public String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public int getInt(String key) {
        return Integer.parseInt(get(key));
    }

    public int getInt(String key, int defaultValue) {
        return Integer.parseInt(get(key, String.valueOf(defaultValue)));
    }

    public boolean getBoolean(String key) {
        return Boolean.parseBoolean(get(key));
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(get(key, String.valueOf(defaultValue)));
    }

    public String getServer() {
        return server;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public int getCalibrationPort() {
        return calibrationPort;
    }

    public int getCalibrationPeriodSec() {
        return calibrationPeriodSec;
    }

    public int getFlushPeriodMs() {
        return flushPeriodMs;
    }

    public String getClientId() {
        return clientId;
    }

    public int getMaxBufferedRecords() {
        return maxBufferedRecords;
    }

    public boolean isUseCompression() {
        return useCompression;
    }

    private void load(InputStream is, Properties defaultProperties) throws IOException {
        properties.load(is);
        loadDefaultProperties(defaultProperties);
        merge(defaultProperties, properties);
        initialize();
    }

    private void loadDefaultProperties(Properties properties) {
        properties.setProperty(SERVER, "localhost");
        properties.setProperty(PORT, "5676");
        properties.setProperty(CALIBRATION_PORT, "5677");
        properties.setProperty(CALIBRATION_PERIOD_SEC, "10");
        properties.setProperty(FLUSH_PERIOD_MS, "1000");
        properties.setProperty(CLIENT_ID, "unknown");
        properties.setProperty(MAX_BUFFERED_RECORDS, "1000000");
        properties.setProperty(USE_COMPRESSION, "true");
        properties.setProperty(HOST_NAME, getHostName());
    }

    private void merge(Properties defaultProperties, Properties targetProperties) {
        for(Object key : defaultProperties.keySet()) {
            if (!targetProperties.containsKey(key)) {
                targetProperties.setProperty((String) key, defaultProperties.getProperty((String) key));
            }
        }
    }

    private void initialize() {
        server = get(SERVER);
        hostname = get(HOST_NAME);
        port = getInt(PORT);
        calibrationPort = getInt(CALIBRATION_PORT);
        calibrationPeriodSec = getInt(CALIBRATION_PERIOD_SEC);
        flushPeriodMs = getInt(FLUSH_PERIOD_MS);
        clientId = get(CLIENT_ID);
        maxBufferedRecords = getInt(MAX_BUFFERED_RECORDS);
        useCompression = getBoolean(USE_COMPRESSION);
    }

    private String getHostName() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new AssertionError("Can't get localhost?");
        }
        return hostname;
    }

    private void close(Closeable is) {
        try {
            if (is != null) {
                is.close();
            }
        } catch (IOException ioe) {
            // ignore
        }
    }
}