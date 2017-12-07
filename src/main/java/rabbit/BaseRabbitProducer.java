package rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import core.BaseProducer;
import core.Message;
import utils.PropFileReader;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static core.BenchMarkingConstants.PRODUCER_ROLE_TYPE;
import static java.lang.String.valueOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static rabbit.RabbitProperties.*;
import static utils.Utils.getNodeIdPrefix;
import static utils.Utils.toJson;

public abstract class BaseRabbitProducer extends BaseProducer {

    private final Connection connection;
    private final Channel channel;
    private final String exchangeName;
    private final String exchangeType;
    private final Boolean isDurableExchange;
    private final String routingKey;
    private Long lastAckDeliveryTag = null;

    BaseRabbitProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) throws IOException, TimeoutException {
        super(id, propFileReader, atomicLong);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(propFileReader.getStringValue(HOST));
        factory.setUsername(propFileReader.getStringValue(USER_NAME));
        factory.setPassword(propFileReader.getStringValue(PASSWORD));
        connection = factory.newConnection();
        channel = connection.createChannel();
        String prefix = getNodeIdPrefix(PRODUCER_ROLE_TYPE, this.id);
        boolean shouldAck = propFileReader.getBooleanValue(prefix + PRODUCER_ACK, false);
        if (shouldAck) {
            channel.confirmSelect();
        }
        exchangeName = propFileReader.getStringValue(prefix + EXCHANGE_NAME);
        exchangeType = propFileReader.getStringValue(prefix + EXCHANGE_TYPE);
        routingKey = propFileReader.getStringValue(prefix + ROUTING_KEY);
        isDurableExchange = propFileReader.getBooleanValue(prefix + EXCHANGE_IS_DURABLE, false);

        //TODO: experiment with auto delete
        channel.exchangeDeclare(exchangeName, exchangeType, isDurableExchange);
        addCallBacks();
    }

    private void addCallBacks() {
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                synchronized (this) {
//                    StringBuilder sb = new StringBuilder();
//                    sb.append(lastAckDeliveryTag).append(",")
//                            .append(deliveryTag).append(multiple);
                    long delta = deliveryTag;
                    if (lastAckDeliveryTag != null) {
                        delta = delta - lastAckDeliveryTag;
                    }
                    lastAckDeliveryTag = deliveryTag;
//                    System.out.println(sb);
                    stats.incrementAckCountBy(delta);
                }
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                //TODO: Fix this to handle multiple
                stats.incrementFailCount();
            }
        });
    }

    @Override
    protected void doStop() {
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
    protected void doProduce(Message message) {
        message.setText(getMessageText());
        message.setOrderKey(valueOf(channel.getChannelNumber()));
        try {
            channel.basicPublish(this.exchangeName, this.routingKey, null, toJson(message).getBytes(UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected abstract String getMessageText();
}
