package utils;

/**
 * Created by Maharia
 */
public interface PropertiesReader {

    /**
     * @param propKey Key for which value is to be fetched from properties file
     * @return String value or null
     */
    String getStringValue(String propKey);

    Boolean getBooleanValue(String propKey);

    /**
     * @param propKey Key for which value is to be fetched from properties file
     * @return Integer Value
     */
    Integer getIntegerValue(String propKey);

    /**
     * @param propKey Key for which value is to be fetched from properties file
     * @return Long Value
     */
    Long getLongValue(String propKey);

    /**
     * @param propKey Key for which value is to be fetched from properties file
     * @return Double Value
     */
    Double getDoubleValue(String propKey);

    //Below Methods work as the above ones but returns a default value in case of null. Default value can be null too.

    String getStringValue(String propKey, String defaultValue);

    Boolean getBooleanValue(String propKey, Boolean defaultValue);

    Integer getIntegerValue(String propKey, Integer defaultValue);

    Long getLongValue(String propKey, Long defaultValue);

    Double getDoubleValue(String propKey, Double defaultValue);

}
