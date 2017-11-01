package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Maharia
 */
public class PropFileReader extends BasePropertyReader {
    private Properties properties;

    public PropFileReader(String propFileName) {
        InputStream stream;
        stream = PropFileReader.class.getClassLoader().getResourceAsStream(propFileName);
        properties = new Properties();
        try {
            properties.load(stream);
        } catch (IOException e) {
            throw new RuntimeException("[FATAL] Unable to load properties file");
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                //suppress
                //todo add logging here later on
            }
        }
    }

    @Override
    public String getStringValue(String propKey) {
        return (String) properties.get(propKey);
    }

}
