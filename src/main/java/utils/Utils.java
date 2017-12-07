package utils;

import com.google.gson.Gson;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.StandardMetricsCollector;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Created by Maharia
 */
@SuppressWarnings("WeakerAccess")
public class Utils {

    /**
     * GSON is thread safe, and can be used as a static variable.
     */
    public static final Gson GSON_INSTANCE = new Gson();
    public static final int MAX_BYTE_SIZE = 1024;
    public static final int MIN_BYTE_SIZE = 64;


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
//            System.out.println(declaredField.getName());
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

    public static String getNodeIdPrefix(String role, int id) {
        return role + "_" + id + ".";
    }

    public static String generateMessageText(int messageSize, int charSize) {
        int numberOfChars = messageSize / charSize;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numberOfChars; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    public static String generateHeterogenousText(Random random, int charSize) {
        int size = random.nextInt((MAX_BYTE_SIZE - MIN_BYTE_SIZE) + 1) + MIN_BYTE_SIZE;
        return generateMessageText(size, charSize);
    }

    public static String getStackTrace(Throwable t) {
        if (t == null) {
            return "<no exception info>";
        }
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    public static void main(String[] args) {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setMetricsCollector(new StandardMetricsCollector());
    }
}
