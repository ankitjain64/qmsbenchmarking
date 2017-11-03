package kafka;

import core.Stats;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

@SuppressWarnings("WeakerAccess")
public class KafkaProduceCallBack implements Callback {

    private Stats stats;

    public KafkaProduceCallBack(Stats stats) {
        this.stats = stats;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            stats.incrementFailCount();
        } else {
            stats.incrementAckCount();
        }
    }
}
