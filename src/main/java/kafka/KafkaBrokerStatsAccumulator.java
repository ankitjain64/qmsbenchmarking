package kafka;

import core.Message;
import core.Stats;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class KafkaBrokerStatsAccumulator implements Runnable {
    private KafkaProducer<String, Message> producer;
    private boolean flag;
    private FileWriter fileWriter;

    public KafkaBrokerStatsAccumulator(KafkaProducer<String, Message> producer) {
        this.producer = producer;
        this.flag = true;
    }

    @Override
    public void run() {
        try {
            fileWriter = new FileWriter
                    ("/home/ubuntu/project/output/kafka/broker_metric.txt");
            fileWriter.write(Stats.getCsvHeaders());
        } catch (IOException e) {
            System.out.println("Unable to open file");
            System.exit(-1);
        }
        while (this.flag) {
            Map<MetricName, ? extends Metric> metrics = producer.metrics();
            try {
                printMetrics(metrics);
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                System.out.println("Thread interupt");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void printMetrics(Map<MetricName, ? extends Metric> metrics) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            MetricName key = entry.getKey();
            Metric value = entry.getValue();
            sb.append(key.name()).append(",").append(value.value()).append
                    ("\n");
        }
        fileWriter.write(sb.toString());
        fileWriter.flush();
    }

    public void stop() {
        this.flag = false;
    }


    static class MetricPair {
        private final MetricName metricName;
        private final Metric metric;

        MetricPair(MetricName metricName, Metric metric) {
            this.metricName = metricName;
            this.metric = metric;
        }

        public String toString() {
            return metricName.group() + "." + metricName.name();
        }
    }
}
