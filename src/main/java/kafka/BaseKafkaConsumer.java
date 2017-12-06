package kafka;

import core.BaseConsumer;
import core.BenchMarkingConstants;
import core.Consumer;
import core.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import utils.PropFileReader;
import utils.Utils;

import java.util.List;
import java.util.Properties;

import static core.BenchMarkingConstants.CONSUMER_ROLE_TYPE;
import static kafka.KafkaProperties.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static utils.Utils.getNodeIdPrefix;

//https://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-kafka-0-9-consumer-client/

/**
 * Created by Maharia
 */
public class BaseKafkaConsumer extends BaseConsumer implements Consumer {

    private final KafkaConsumer<String, Message> consumer;
    private final Long pollTimeoutMs;
    private List<String> topics;
    private Long perRecordsConsumptionMs;

    BaseKafkaConsumer(int id, PropFileReader propFileReader) {
        super(id, propFileReader);
        consumer = new KafkaConsumer<>(extractBaseKafkaConsumerProperties(propFileReader));
        //TODO: Fix me and change config
        String topicsCsv = propFileReader.getStringValue(CONSUMER_TOPIC_SUBSCRIPTION);
        topics = Utils.parseString(topicsCsv, ",");
        String prefix = getNodeIdPrefix(CONSUMER_ROLE_TYPE, this.id);
        perRecordsConsumptionMs = propFileReader.getLongValue(prefix + PER_RECORD_CONSUMPTION_TIME_MS, 0L);
        pollTimeoutMs = propFileReader.getLongValue(prefix + POLL_TIMEOUT_MS, 0L);
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
                ConsumerRecords<String, Message> records = consumer.poll(pollTimeoutMs);
                if (records != null && !records.isEmpty()) {
                    for (ConsumerRecord<String, Message> record : records) {
                        updateStats(record.value());
                        if (Long.compare(perRecordsConsumptionMs, 0) != 0) {
                            Thread.sleep(perRecordsConsumptionMs);
                        }
                    }
                    consumer.commitSync();
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
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaMessageDeserializer.class.getName());
        /*
         * Bootstrap server to connect to broker, not all need to be specified
         */
        properties.setProperty(BOOTSTRAP_SERVERS, propFileReader.getStringValue(BOOTSTRAP_SERVERS));
        String prefix = getNodeIdPrefix(CONSUMER_ROLE_TYPE, this.id);
        properties.setProperty(GROUP_ID, propFileReader.getStringValue(prefix + GROUP_ID));
        properties.setProperty(SESSION_TIMEOUT_MS, propFileReader.getStringValue(prefix + SESSION_TIMEOUT_MS, "10000"));
        properties.setProperty(HEARTBEAT_INTERVAL_MS, propFileReader.getStringValue(prefix + HEARTBEAT_INTERVAL_MS, "3000"));
        Boolean autCommit = propFileReader.getBooleanValue(prefix + ENABLE_AUTO_COMMIT, true);
        properties.setProperty(ENABLE_AUTO_COMMIT, autCommit.toString());
        Integer integerValue = propFileReader.getIntegerValue(BenchMarkingConstants.NODE_COUNT);
        properties.setProperty("fetch.max.bytes", String.valueOf((1024l * 1024l * 1024l) / integerValue));
        if (autCommit) {
            properties.setProperty(AUTO_COMMIT_INTERVAL, propFileReader.getStringValue(prefix + AUTO_COMMIT_INTERVAL, "5000"));
        }
        return properties;
    }
}
