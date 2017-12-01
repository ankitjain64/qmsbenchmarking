package flume;

import core.BaseProducer;
import core.Message;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.*;
import org.apache.flume.event.EventBuilder;
import utils.PropFileReader;

import java.nio.charset.Charset;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static flume.FlumeProperties.*;
import static utils.Utils.toJson;

public abstract class BaseRpcFlumeProducer extends BaseProducer {
    public static final String DEFAULT = "default";
    public static final String LOAD_BALANCE = "load.balance";
    private final Properties connectProps;
    private RpcClient client;


    public BaseRpcFlumeProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) {
        super(id, propFileReader, atomicLong);
        String hostname = propFileReader.getStringValue(FLUME_HOST);//space
        // seperated host:port
        connectProps = new Properties();
        String clientType = extractClientType(connectProps.getProperty(PRODUCER_TYPE, DEFAULT));
        connectProps.setProperty(RpcClientConfigurationConstants
                .CONFIG_CLIENT_TYPE, clientType);
        connectProps.setProperty(RpcClientConfigurationConstants
                .CONFIG_BATCH_SIZE, propFileReader.getStringValue(BATCH_SIZE,
                "100"));
        connectProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS,
                hostname);
        this.client = RpcClientFactory.getInstance(connectProps);
    }

    @Override
    protected void doStop() {
        client.close();
    }

    @Override
    protected void doProduce(Message message) {
        message.setText(getText());
        Event event = EventBuilder.withBody(toJson(message), Charset.forName("UTF-8"));
        try {
            client.append(event);
        } catch (EventDeliveryException e) {
            // clean up and recreate the client
            client.close();
            client = null;
            System.out.println("FAILURE!!!!!!!!!!!!!!!");
            client = RpcClientFactory.getInstance(connectProps);
        }
    }

    public abstract String getText();

    private String extractClientType(String clientType) {
        if (DEFAULT.equals(clientType)) {
            return NettyAvroRpcClient.class.getCanonicalName();
        } else if (LOAD_BALANCE.equals(clientType)) {
            return LoadBalancingRpcClient.class.getCanonicalName();
        } else {
            throw new IllegalArgumentException("Invalid client Type: " +
                    "" + clientType);
        }
    }
}
