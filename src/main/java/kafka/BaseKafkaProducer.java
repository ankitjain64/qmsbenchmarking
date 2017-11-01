package kafka;

import core.BaseProducer;
import core.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.PropFileReader;

import java.util.Properties;

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
    protected final String topic;

    public BaseKafkaProducer(int id, PropFileReader propFileReader) {
        super(id, propFileReader);
        producer = new KafkaProducer<>(extractBaseKafkaProducerProperties(propFileReader));
        String prefix = PRODUCER_ROLE_TYPE + "_" + this.id + ".";
        topic = propFileReader.getStringValue(prefix + PRODUCER_TOPIC);

    }

    @Override
    public void doStop() {
        producer.flush();
        producer.close();
    }

    protected void doProduce(Message message) {
        String key = this.id + "_" + this.totalMessageSentCount;
        producer.send(new ProducerRecord<String, Message>(this.topic, message));
    }

    protected Properties extractBaseKafkaProducerProperties(PropFileReader propFileReader) {
        Properties properties = new Properties();
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(BOOTSTRAP_SERVERS, propFileReader.getStringValue(BOOTSTRAP_SERVERS));
        String prefix = PRODUCER_ROLE_TYPE + "_" + this.id + ".";
        properties.setProperty(CLIENT_ID, propFileReader.getStringValue(prefix + CLIENT_ID));
        String key = PRODUCER_ROLE_TYPE + "." + TYPE;
        properties.setProperty(key, propFileReader.getStringValue(prefix + TYPE));
        //not setting batch and ack properties
        //max in flight and retry config
        return properties;
    }

    protected abstract String getMessage();

}
