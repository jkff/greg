package org.greg.client;

import junit.framework.TestCase;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.Properties;

import static org.greg.client.Configuration.*;

/**
 * @author Anton Panasenko
 * Date: 25.12.10
 */
public class ConfigurationTest extends TestCase {
    private String[] keys = {SERVER, HOST_NAME, PORT, CALIBRATION_PORT, CALIBRATION_PERIOD_SEC, FLUSH_PERIOD_MS,
            CLIENT_ID, MAX_BUFFERED_RECORDS, USE_COMPRESSION};

    public void testPropertiesFiles() throws IOException, IntrospectionException {
        Properties properties = new Properties();
        properties.load(getClass().getResourceAsStream("/greg.properties"));

        Configuration config = Configuration.INSTANCE;

        for(String key: keys) {
            String value = properties.getProperty(key);
            if (value != null) {
                assertEquals(value, config.get(key));
            }
        }
    }
}