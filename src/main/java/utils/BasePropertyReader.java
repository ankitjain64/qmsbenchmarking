package utils;

import static utils.Utils.getValueOrDefault;

/**
 * Created by Maharia
 */
public abstract class BasePropertyReader implements PropertiesReader {

    @Override
    public Integer getIntegerValue(String propKey) {
        String strValue = getStringValue(propKey);
        return Utils.getSafeIntValue(strValue);
    }

    @Override
    public Boolean getBooleanValue(String propKey) {
        String stringValue = getStringValue(propKey);
        if ("T".equalsIgnoreCase(stringValue) || "true".equalsIgnoreCase(stringValue)) {
            return Boolean.TRUE;
        } else if ("F".equalsIgnoreCase(stringValue) || "false".equalsIgnoreCase(stringValue)) {
            return Boolean.FALSE;
        } else {
            return null;
        }
    }

    @Override
    public Long getLongValue(String propKey) {
        String strValue = getStringValue(propKey);
        return Utils.getSafeLongValue(strValue);
    }

    @Override
    public Double getDoubleValue(String propKey) {
        String strValue = getStringValue(propKey);
        return Utils.getSafeDoubleValue(strValue);
    }

    @Override
    public String getStringValue(String propKey, String defaultValue) {
        return getValueOrDefault(getStringValue(propKey), defaultValue);
    }

    @Override
    public Integer getIntegerValue(String propKey, Integer defaultValue) {
        return getValueOrDefault(getIntegerValue(propKey), defaultValue);
    }

    @Override
    public Long getLongValue(String propKey, Long defaultValue) {
        return getValueOrDefault(getLongValue(propKey), defaultValue);
    }

    @Override
    public Double getDoubleValue(String propKey, Double defaultValue) {
        return getValueOrDefault(getDoubleValue(propKey), defaultValue);
    }

    @Override
    public Boolean getBooleanValue(String propKey, Boolean defaultValue) {
        return getValueOrDefault(getBooleanValue(propKey), defaultValue);
    }
}
