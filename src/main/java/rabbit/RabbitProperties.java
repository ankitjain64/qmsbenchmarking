package rabbit;

@SuppressWarnings("WeakerAccess")
public class RabbitProperties {

    public static final String HOST = "host";
    public static final String QUEUE_NAME = "queue.name";

    /**
     * Producer Keys
     */
    public static final String EXCHANGE_NAME="exchange.name";
    public static final String EXCHANGE_TYPE="exchange.type";
    public static final String EXCHANGE_IS_DURABLE="exchange.is.durable";
    public static final String ROUTING_KEY="routing.key";
}