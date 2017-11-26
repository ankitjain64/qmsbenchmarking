package flume;

import core.BaseProducer;
import core.Message;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import utils.PropFileReader;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;

import static flume.FlumeProperties.FLUME_HOST;
import static flume.FlumeProperties.FLUME_PORT;
import static utils.Utils.toJson;

public abstract class BaseFlumeProducer extends BaseProducer {
    private final String hostname;
    private final int port;
    private RpcClient client;


    public BaseFlumeProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) {
        super(id, propFileReader, atomicLong);
        this.hostname = propFileReader.getStringValue(FLUME_HOST);
        this.port = propFileReader.getIntegerValue(FLUME_PORT);
        this.client = RpcClientFactory.getDefaultInstance(hostname, port);
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
            client = RpcClientFactory.getDefaultInstance(hostname, port);
        }
    }

    public abstract String getText();
}
