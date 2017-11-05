package kafka;

import core.BaseProducer;
import core.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.PropFileReader;
import utils.Utils;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static core.BenchMarkingConstants.PRODUCER_ROLE_TYPE;
import static kafka.KafkaProperties.*;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

//http://cloudurable.com/blog/kafka-tutorial-kafka-producer-advanced-java-examples/index.html

/**
 * Created by Maharia
 */
public abstract class BaseKafkaProducer extends BaseProducer {

    protected final KafkaProducer<String, Message> producer;
    private final String topic;
    private final int partition;
    private static KafkaProduceCallBack callback;

    BaseKafkaProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) {
        super(id, propFileReader, atomicLong);
        producer = new KafkaProducer<>(extractBaseKafkaProducerProperties(propFileReader));
        String prefix = Utils.getNodeIdPrefix(PRODUCER_ROLE_TYPE, this.id);
        topic = propFileReader.getStringValue(prefix + PRODUCER_TOPIC);
        partition = propFileReader.getIntegerValue(prefix + PARTIOTION_ID, 1);
        callback = KafkaProduceCallBack.getInstance(this.stats);
    }

    @Override
    public void doStop() {
        producer.flush();
        producer.close();
    }

    protected void doProduce(Message message) {
        String key = this.id + "_" + this.totalMessageSentCount;
        message.setText(getMessageText());
        //need to check order across partitions
        message.setOrderKey(String.valueOf(this.partition));
        ProducerRecord<String, Message> record = new ProducerRecord<>(this.topic, this.partition, message.getpTs(), key, message);
        producer.send(record, callback);
    }

    private Properties extractBaseKafkaProducerProperties(PropFileReader propFileReader) {
        Properties properties = new Properties();
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, KafkaMessageSerializer.class.getName());
        properties.setProperty(BOOTSTRAP_SERVERS, propFileReader.getStringValue(BOOTSTRAP_SERVERS));
        String prefix = Utils.getNodeIdPrefix(PRODUCER_ROLE_TYPE, this.id);
        properties.setProperty(CLIENT_ID, propFileReader.getStringValue(prefix + CLIENT_ID, String.valueOf(this.id)));
        properties.setProperty(ACKS, propFileReader.getStringValue(prefix + ACKS, "1"));
        properties.setProperty(LINGER_MS, propFileReader.getStringValue(prefix + LINGER_MS, "0"));
        properties.setProperty(BATCH_SIZE, propFileReader.getStringValue(prefix + BATCH_SIZE, "16384"));
        properties.setProperty(COMPRESSION_TYPE, propFileReader.getStringValue(prefix + COMPRESSION_TYPE, "none"));
        properties.setProperty(REQUEST_TIMEOUT_MS, propFileReader.getStringValue(prefix + REQUEST_TIMEOUT_MS, "30000"));
        return properties;
    }

    protected abstract String getMessageText();
}
