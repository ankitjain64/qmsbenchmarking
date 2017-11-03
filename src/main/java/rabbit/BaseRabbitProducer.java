package rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import core.BaseProducer;
import core.Message;
import utils.PropFileReader;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static core.BenchMarkingConstants.CONSUMER_ROLE_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static rabbit.RabbitProperties.*;
import static utils.Utils.getNodeIdPrefix;
import static utils.Utils.toJson;

public abstract class BaseRabbitProducer extends BaseProducer {

    private final Connection connection;
    private final Channel channel;
    private final String exchangeName;
    private final String exchahgeType;
    private final Boolean isDurbleExchange;
    private final String routngKey;

    BaseRabbitProducer(int id, PropFileReader propFileReader) throws IOException, TimeoutException {
        super(id, propFileReader);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(propFileReader.getStringValue(HOST));
        connection = factory.newConnection();
        channel = connection.createChannel();
        String prefix = getNodeIdPrefix(CONSUMER_ROLE_TYPE, this.id);
        exchangeName = propFileReader.getStringValue(prefix + EXCHANGE_NAME);
        exchahgeType = propFileReader.getStringValue(prefix + EXCHANGE_TYPE);
        routngKey = propFileReader.getStringValue(prefix + ROUTING_KEY);
        isDurbleExchange = propFileReader.getBooleanValue(prefix + EXCHANGE_IS_DURABLE, false);

        //TODO: experiment with auto delete
        channel.exchangeDeclare(exchangeName, exchahgeType, isDurbleExchange);
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
        try {
            channel.basicPublish(this.exchangeName, this.routngKey, null, toJson(message).getBytes(UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected abstract String getMessageText();
}
