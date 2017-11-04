package utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Maharia
 */
public class PropFileReader extends BasePropertyReader {
    private Properties properties;

    public PropFileReader(String propFileName) {
        InputStream stream = null;
        try {
            stream = new FileInputStream(propFileName);
            properties = new Properties();
            properties.load(stream);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public String getStringValue(String propKey) {
        return (String) properties.get(propKey);
    }

}
