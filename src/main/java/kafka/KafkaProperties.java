package kafka;

/**
 * Created by Maharia
 */
@SuppressWarnings("WeakerAccess")
public class KafkaProperties {
    //comma seperated list of topics to subscribe to
    public static final String CONSUMER_TOPIC_SUBSCRIPTION = "kafka.consumer.topics";
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

    /**
     * Consumer Properties
     */
    public static final String GROUP_ID = "group.id";
    public static final String SESSION_TIMEOUT_MS = "session.timeout.ms";
    public static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
    public static final String AUTO_COMMIT_INTERVAL = "auto.commit.interval";
    public static final String PER_RECORD_CONSUMPTION_TIME_MS = "per.record.consumption.time.ms";
    public static final String POLL_TIMEOUT_MS = "poll.timeout.ms";
    public static final String HEARTBEAT_INTERVAL_MS= "heartbeat.interval.ms";
    //explore partition assignment strategy


    /**
     * Producer Properties
     */
    //Properties for batching
    public static final String LINGER_MS = "linger.ms";//default 0
    public static final String BATCH_SIZE = "batch.size";//default 16384
    public static final String COMPRESSION_TYPE = "compression.type";//none
    public static final String REQUEST_TIMEOUT_MS = "request.timeout.ms";//default 30,000

    //not added buffer memory max in flight and retry config

    public static final String CLIENT_ID = "client.id";
//    public static final String TYPE = "type";//cant remember its usage
    public static final String PRODUCER_TOPIC = "topic";
    public static final String PARTIOTION_ID="partition.id";
    public static final String ACKS="acks";//default 1: valid:0,1,all(-1)


}
