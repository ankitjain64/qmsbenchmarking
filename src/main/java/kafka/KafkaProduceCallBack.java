package kafka;

import core.Stats;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import utils.Utils;

public class KafkaProduceCallBack implements Callback {

    private final Stats stats;

    KafkaProduceCallBack(Stats stats) {
        this.stats = stats;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            stats.incrementFailCount();
        } else {
            stats.incrementAckCountAndLatency(Utils.getCurrentTime() - metadata.timestamp());
        }
    }
}
