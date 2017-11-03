package core;

import utils.PropFileReader;

import java.util.HashMap;
import java.util.Map;

import static core.BenchMarkingConstants.*;
import static utils.Utils.getCurrentTime;

/**
 * Created By Maharia
 */
public abstract class BaseConsumer implements Consumer {

    private PropFileReader propFileReader;
    protected int id;
    protected final Stats stats;
    private final StatsAccumulator statsAccumulator;
    private final String statsOutputPath;
    private Message lastMessage;

    private Map<Integer, Map<String, Message>> producerIdVsOrderKeyVsMessage;

    public BaseConsumer(int id, PropFileReader propFileReader) {
        this.id = id;
        this.propFileReader = propFileReader;
        long currentTime = getCurrentTime();
        this.stats = new Stats(currentTime);
        String prefix = CONSUMER_ROLE_TYPE + "_" + id + ".";
        this.statsOutputPath = propFileReader.getStringValue(prefix + STATS_OUTPUT_PATH);
        long statsAccumulationTime = propFileReader.getLongValue(prefix + STATS_ACCUMULATION_INTERVAL, 0L);
        this.producerIdVsOrderKeyVsMessage = new HashMap<>();
        if (statsAccumulationTime == 0) {
            throw new IllegalArgumentException("Stats accumulation time >0");
        }
        statsAccumulator = new StatsAccumulator(this, statsAccumulationTime, this.statsOutputPath);
        new Thread(statsAccumulator).run();
    }

    protected void updateStats(Message message) {
        synchronized (this.stats) {
            Map<String, Message> orderKeyVsMessage = producerIdVsOrderKeyVsMessage.get(message.getpId());
            boolean isOutOfOrder = false;
            if (orderKeyVsMessage == null) {
                orderKeyVsMessage = new HashMap<>();
                producerIdVsOrderKeyVsMessage.put(message.getpId(), orderKeyVsMessage);
            }
            Message existing = orderKeyVsMessage.get(message.getOrderKey());
            if (existing != null) {
                isOutOfOrder = message.constructMessageId().compareTo(existing.constructMessageId()) < 0;
            }
            orderKeyVsMessage.put(message.getOrderKey(), message);
            stats.incrementRcvCount();
            stats.setOutofOrder(isOutOfOrder);
        }
    }

    @Override
    public void stop() {
        statsAccumulator.stop(this.getCurrentStatsSnapShot());
        doStop();
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public Stats getCurrentStatsSnapShot() {
        synchronized (this.stats) {
            return this.stats.createSnapShot(getCurrentTime());
        }
    }

    protected abstract void doStop();
}
