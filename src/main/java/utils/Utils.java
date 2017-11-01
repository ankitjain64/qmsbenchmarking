package utils;

import com.google.gson.Gson;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by Maharia
 */
@SuppressWarnings("WeakerAccess")
public class Utils {

    /**
     * GSON is thread safe, and can be used as a static variable.
     */
    public static final Gson GSON_INSTANCE = new Gson();


    public static <T> boolean isNull(T input) {
        return input == null;
    }

    public static <T> boolean isNotNull(T input) {
        return !isNull(input);
    }

    public static <T> T getValueOrDefault(T propValue, T defaultValue) {
        if (Utils.isNull(propValue)) {
            return defaultValue;
        }
        return propValue;
    }

    public static Integer getSafeIntValue(String stringValue) {
        try {
            return Integer.valueOf(stringValue);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    public static Long getSafeLongValue(String stringValue) {
        try {
            return Long.valueOf(stringValue);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    public static Double getSafeDoubleValue(String stringValue) {
        try {
            return Double.valueOf(stringValue);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    public static List<String> parseString(String input, String delim) {
        String[] split = input.split(delim);
        return Arrays.asList(split);
    }

    public static int getCharByteSize() {
        Field[] declaredFields = Character.class.getDeclaredFields();
        for (Field declaredField : declaredFields) {
            System.out.println(declaredField.getName());
            if (declaredField.getName().equals("SIZE")) {
                try {
                    int bitsSize = (int) declaredField.get(null);
                    return bitsSize / 8;
                } catch (IllegalAccessException e) {
                    System.out.println("Exception while getting size");
                    e.printStackTrace();
                }
            }
        }
        throw new RuntimeException("Unable to determine char size");
    }

    public static <T> String toJson(T object) {
        return GSON_INSTANCE.toJson(object);
    }

    public static <T> T fromJson(byte[] data, Class<T> clazz) {
        return GSON_INSTANCE.fromJson(new String(data), clazz);
    }

    public static long getCurrentTime() {
        return new Date().getTime();
    }
}
