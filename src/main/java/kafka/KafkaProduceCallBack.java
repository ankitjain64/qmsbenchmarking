package kafka;

import core.Stats;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProduceCallBack implements Callback {

    private Stats stats;

    KafkaProduceCallBack(Stats stats) {
        this.stats = stats;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        synchronized (this.stats) {
            if (exception != null) {
                stats.incrementFailCount();
            } else {
                stats.incrementAckCount();
            }
        }
    }
}
