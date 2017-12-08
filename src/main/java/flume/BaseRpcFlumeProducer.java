package flume;

import core.BaseProducer;
import core.Message;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.*;
import org.apache.flume.event.EventBuilder;
import utils.PropFileReader;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static flume.FlumeProperties.*;
import static utils.Utils.toJson;

public abstract class BaseRpcFlumeProducer extends BaseProducer {
    public static final String DEFAULT = "default";
    public static final String LOAD_BALANCE = "load.balance";
    private final Properties connectProps;
    private final int batchSize;
    private RpcClient client;
    private List<Event> eventList;


    public BaseRpcFlumeProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) {
        super(id, propFileReader, atomicLong);
        connectProps = new Properties();
        String hostname = propFileReader.getStringValue(FLUME_HOSTS_NAME);//space
        String[] split = hostname.split("\\s+");
        for (String s : split) {
            String hostPropKey = RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + s;
            connectProps.setProperty(hostPropKey, propFileReader.getStringValue(hostPropKey));
        }

        // seperated host:port
        String clientType = extractClientType(connectProps.getProperty(PRODUCER_TYPE, DEFAULT));
        connectProps.setProperty(RpcClientConfigurationConstants
                .CONFIG_CLIENT_TYPE, clientType);
        String batchSizeStr = propFileReader.getStringValue(BATCH_SIZE,
                "100");
        batchSize = Integer.parseInt(batchSizeStr);
        connectProps.setProperty(RpcClientConfigurationConstants
                .CONFIG_BATCH_SIZE, batchSizeStr);
        connectProps.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS,
                hostname);
        this.client = RpcClientFactory.getInstance(connectProps);
        eventList = new ArrayList<>();
    }

    @Override
    protected void doStop() {
        if (eventList.size() != 0) {
            sendBatch();
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            System.out.println("Interupt while thread sleep");
            e.printStackTrace();
        }
        client.close();
    }

    @Override
    protected void doProduce(Message message) {
        message.setText(getText());
        Event event = EventBuilder.withBody(toJson(message), Charset.forName("UTF-8"));
        eventList.add(event);
        if (eventList.size() == batchSize) {
            sendBatch();
        }
    }

    private void sendBatch() {
        try {
            client.appendBatch(eventList);
            this.stats.incrementAckCountBy(eventList.size());
            eventList = new ArrayList<>();
        } catch (EventDeliveryException e) {
            // clean up and recreate the client
            client.close();
            client = null;
            e.printStackTrace();
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
