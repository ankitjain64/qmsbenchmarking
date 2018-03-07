package kafka;

import core.Message;
import org.apache.kafka.common.serialization.Deserializer;
import utils.Utils;

import java.util.Map;

public class KafkaMessageDeserializer implements Deserializer<Message> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Message deserialize(String topic, byte[] data) {
        Message message = Utils.fromJson(data, Message.class);
        message.setcTs(Utils.getCurrentTime());
        return message;
    }

    @Override
    public void close() {

    }
}
