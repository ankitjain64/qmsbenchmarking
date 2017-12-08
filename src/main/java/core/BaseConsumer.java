package core;

import utils.PropFileReader;

import java.util.HashMap;
import java.util.Map;

import static utils.Utils.getCurrentTime;

/**
 * Created By Maharia
 */
public abstract class BaseConsumer implements Consumer {

    private PropFileReader propFileReader;
    protected int id;
    protected final Stats stats;

    private Map<Integer, Map<String, Message>> producerIdVsOrderKeyVsMessage;
    private Message lastMessage;

    public BaseConsumer(int id, PropFileReader propFileReader) {
        this.id = id;
        this.propFileReader = propFileReader;
        long currentTime = getCurrentTime();
        this.stats = new Stats(currentTime);
        this.producerIdVsOrderKeyVsMessage = new HashMap<>();
    }

    protected synchronized void updateStats(Message message) {
        synchronized (this.stats) {
            //System.out.println("Updating stats");
            Map<String, Message> orderKeyVsMessage = producerIdVsOrderKeyVsMessage.get(message.getpId());
            boolean isOutOfOrder = false;
            if (orderKeyVsMessage == null) {
                orderKeyVsMessage = new HashMap<>();
                producerIdVsOrderKeyVsMessage.put(message.getpId(), orderKeyVsMessage);
            }
            Message existing = orderKeyVsMessage.get(message.getOrderKey());
            if (existing != null) {
                isOutOfOrder = Long.compare(message.getNum(), existing.getNum()) < 0;
            }
            boolean isGlobalOutOfOrder = false;
            if (lastMessage != null) {
                isGlobalOutOfOrder = Long.compare(message.getNum(), lastMessage.getNum()) < 0;
            }
            lastMessage = message;
            orderKeyVsMessage.put(message.getOrderKey(), message);
            stats.incrementRcvCountAndLatency(message.getDelta());
            stats.setOutofOrder(isOutOfOrder);
            stats.setGlobalOutOfOrder(isGlobalOutOfOrder);
        }
    }

    @Override
    public void stop() {
        doStop();
    }

    @Override
    public int getId() {
        return id;
    }

    protected abstract void doStop();
}
