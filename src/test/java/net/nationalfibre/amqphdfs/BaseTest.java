package net.nationalfibre.amqphdfs;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class BaseTest 
{
    private Properties properties = null;

    protected String getParameter(String key, String value)
    {
        if (properties != null) {
            return properties.getProperty(key, value);
        }

        try {
            properties      = new Properties();
            InputStream in  = getClass().getResourceAsStream("/test.properties");

            properties.load(in);
            in.close();

        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return properties.getProperty(key, value);
    }
}
