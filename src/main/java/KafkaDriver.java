import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;


//https://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-kafka-0-9-consumer-client/

/**
 * Author: Ankit Maharia
 */
public class KafkaDriver {
    public static void main(String[] args) throws IOException {
        String producerPropFile = args[0];
        String consumerPropFile = args[1];
        KafkaPTest kafkaPTest = new KafkaPTest(producerPropFile);
        int numConsumer = 3;
        final ExecutorService executorService = Executors.newFixedThreadPool(numConsumer);
        final List<KafkaCTest> consumers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            KafkaCTest cTest = new KafkaCTest(consumerPropFile, i);
            consumers.add(cTest);
            executorService.submit(cTest);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (KafkaCTest consumer : consumers) {
                    consumer.shutDown();
                }
                executorService.shutdown();
                try {
                    executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        });
    }


    static class KafkaPTest {

        private final KafkaProducer<String, String> producer;

        KafkaPTest(String propFilePath) throws IOException {
            InputStream resourceAsStream = KafkaPTest.class.getClassLoader()
                    .getResourceAsStream(propFilePath);
            Properties properties = new Properties();
            properties.load(resourceAsStream);
            properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producer = new KafkaProducer<String, String>(properties);
        }

        public void syncProduce(ProducerRecord record) throws ExecutionException, InterruptedException {
            producer.send(record).get();
        }

        public void asyncProduce(ProducerRecord record) {
            producer.send(record);
        }
    }


    static class KafkaCTest implements Runnable {

        private final KafkaConsumer<String, String> consumer;
        private Collection<String> topics;
        private final int id;

        KafkaCTest(String propFilePath, int id) throws IOException {
            this.id = id;
            InputStream resourceAsStream = KafkaPTest.class.getClassLoader()
                    .getResourceAsStream(propFilePath);
            Properties properties = new Properties();
            properties.load(resourceAsStream);
            properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumer = new KafkaConsumer<>(properties);
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(topics);
                //noinspection InfiniteLoopStatement
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                    for (ConsumerRecord<String, String> record : records) {
                        Map<String, Object> data = new HashMap<>();
                        data.put("partition", record.partition());
                        data.put("offset", record.offset());
                        data.put("value", record.value());
                        System.out.println(this.id + ": " + data);
                    }
                }
            } catch (WakeupException ex) {
                //for shutdown
            } finally {
                consumer.close();
            }

        }

        public void shutDown() {
            consumer.wakeup();
        }
    }
}
