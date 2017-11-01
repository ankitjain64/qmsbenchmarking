package kafka;

import core.BaseConsumer;
import core.BaseProducer;
import core.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import utils.PropFileReader;
import utils.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static core.BenchMarkingConstants.*;
import static kafka.KafkaProperties.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

////https://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-kafka-0-9-consumer-client/
public class BaseKafkaConsumer extends BaseConsumer implements Consumer {

    private final KafkaConsumer<String, String> consumer;
    private final Long pollTimeoutMs;
    private List<String> topics;
    private Long perRecordsConsumptionMs;

    public BaseKafkaConsumer(int id, PropFileReader propFileReader) {
        super(id, propFileReader);
        consumer = new KafkaConsumer<>(extractBaseKafkaConsumerProperties(propFileReader));
        String topicsCsv = propFileReader.getStringValue(CONSUMER_TOPIC_SUBSCRIPTION);
        topics = Utils.parseString(topicsCsv, ",");
        String prefix = CONSUMER_ROLE_TYPE + "_" + this.id + ".";
        perRecordsConsumptionMs = propFileReader.getLongValue(prefix + PER_RECORD_CONSUMPTION_TIME_MS, 0L);
        pollTimeoutMs = propFileReader.getLongValue(prefix + POLL_TIMEOUT_MS, 10000L);
        if (pollTimeoutMs == 0) {
            throw new IllegalArgumentException("Poll interval for consumer cannot be zero");
        }
    }

    @Override
    public void doStop() {
        consumer.wakeup();
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);
            //noinspection InfiniteLoopStatement
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    updateStats();
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition:", record.partition());
                    data.put("offset:", record.offset());
                    data.put("value:", record.value());
                    System.out.println(data);
                    if (Long.compare(perRecordsConsumptionMs, 0) != 0) {
                        Thread.sleep(perRecordsConsumptionMs);
                    }
                }
            }
        } catch (WakeupException ex) {
            //for shutdown
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private Properties extractBaseKafkaConsumerProperties(PropFileReader propFileReader) {
        Properties properties = new Properties();
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /**
         * Bootstrap server to connect to broker, not all need to be specified
         */
        properties.setProperty(BOOTSTRAP_SERVERS, propFileReader.getStringValue(BOOTSTRAP_SERVERS));
        String prefix = CONSUMER_ROLE_TYPE + "_" + this.id + ".";
        /**
         * Group id so that group lock can be help so that no other consumer
         * reads the same partition as long as group lock is held. Group
         * cordination done via group co-ordination protocol which uses
         * heartbeat
         */
        properties.setProperty(GROUP_ID, propFileReader.getStringValue(prefix + GROUP_ID));
        /**
         *
         */
        properties.setProperty(SESSION_TIMEOUT_MS, propFileReader.getStringValue(prefix + SESSION_TIMEOUT_MS));
        Boolean autCommit = propFileReader.getBooleanValue(prefix + ENABLE_AUTO_COMMIT, false);
        properties.setProperty(ENABLE_AUTO_COMMIT, autCommit.toString());
        if (autCommit) {
            properties.setProperty(AUTO_COMMIT_INTERVAL, propFileReader.getStringValue(prefix + AUTO_COMMIT_INTERVAL));
        }
        return properties;
    }
}
