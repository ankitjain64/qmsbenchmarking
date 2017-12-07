package rabbit;

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.StandardMetricsCollector;
import core.BaseConsumer;
import core.Message;
import utils.PropFileReader;
import utils.Utils;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static core.BenchMarkingConstants.CONSUMER_ROLE_TYPE;
import static kafka.KafkaProperties.PER_RECORD_CONSUMPTION_TIME_MS;
import static rabbit.RabbitProperties.*;
import static utils.Utils.getNodeIdPrefix;

@SuppressWarnings("FieldCanBeLocal")
public class RabbitConsumer extends BaseConsumer implements Consumer {
    private final Connection connection;
    private final Channel channel;
    private final Long perRecordsConsumptionMs;
    private String queueName;
    private final String exchangeName;
    private final String exchahgeType;
    private final String routngKey;
    private String consumerTag;

    RabbitConsumer(int id, PropFileReader propFileReader) throws IOException, TimeoutException {
        super(id, propFileReader);
        StandardMetricsCollector metrics = new StandardMetricsCollector();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(propFileReader.getStringValue(HOST));
        factory.setUsername(propFileReader.getStringValue(USER_NAME));
        factory.setPassword(propFileReader.getStringValue(PASSWORD));
        factory.setMetricsCollector(metrics);
        connection = factory.newConnection();
        channel = connection.createChannel();
        String prefix = getNodeIdPrefix(CONSUMER_ROLE_TYPE, this.id);
        perRecordsConsumptionMs = propFileReader.getLongValue(prefix + PER_RECORD_CONSUMPTION_TIME_MS, 0L);
        exchangeName = propFileReader.getStringValue(prefix + EXCHANGE_NAME);
        exchahgeType = propFileReader.getStringValue(prefix + EXCHANGE_TYPE);
        routngKey = propFileReader.getStringValue(prefix + ROUTING_KEY);
        queueName = propFileReader.getStringValue(prefix + QUEUE_NAME);
        if (Utils.isNull(queueName)) {
            queueName = channel.queueDeclare().getQueue();
        }
        channel.queueBind(queueName, exchangeName, routngKey);
    }

    @Override
    protected void doStop() {
        try {
            channel.basicCancel(this.consumerTag);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            channel.basicConsume(queueName, false, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //#############################################################
    //------------------Methods from Rabbit Client-----------------
    //#############################################################
    @Override
    public void handleConsumeOk(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    @Override
    public void handleCancelOk(String consumerTag) {
        try {
            channel.close();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {
        //no-op
    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
        //no-op
    }

    @Override
    public void handleRecoverOk(String consumerTag) {
        //no-op
    }

    @Override
    public synchronized void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        Message message = Utils.fromJson(body, Message.class);
        message.setcTs(Utils.getCurrentTime());
        updateStats(message);
        if (Long.compare(perRecordsConsumptionMs, 0) != 0) {
            try {
                Thread.sleep(perRecordsConsumptionMs);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        channel.basicAck(envelope.getDeliveryTag(), false);
    }
}
