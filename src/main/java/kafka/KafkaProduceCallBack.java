package kafka;

import core.Stats;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import utils.Utils;

import java.util.HashMap;
import java.util.Map;

public class KafkaProduceCallBack implements Callback {

    private final Stats stats;

    private static KafkaProduceCallBack callBack;

    private Map<String, Boolean> exceptionVsStackTrace;

    private KafkaProduceCallBack(Stats stats) {
        this.stats = stats;
        exceptionVsStackTrace = new HashMap<>();
    }

    static KafkaProduceCallBack getInstance(Stats stats) {
        if (callBack == null) {
            synchronized (KafkaProduceCallBack.class) {
                if (callBack == null) {
                    callBack = new KafkaProduceCallBack(stats);
                }
            }
        }
        return callBack;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        synchronized (this) {
            if (exception != null) {
                stats.incrementFailCount();
                Boolean isLogged = exceptionVsStackTrace.get(exception.getLocalizedMessage());
                if (isLogged == null || !isLogged) {
                    exceptionVsStackTrace.put(exception.getLocalizedMessage(), true);
                }
                String stackTrace = Utils.getStackTrace(exception);
                System.out.println(stackTrace);
            } else {
                stats.incrementAckCountAndLatency(Utils.getCurrentTime() - metadata.timestamp());
            }
        }
    }
}
