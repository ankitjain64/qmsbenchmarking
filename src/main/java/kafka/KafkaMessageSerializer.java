package kafka;

import core.Message;
import org.apache.kafka.common.serialization.Serializer;
import utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KafkaMessageSerializer implements Serializer<Message> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Message data) {
        return Utils.toJson(data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {

    }
}
