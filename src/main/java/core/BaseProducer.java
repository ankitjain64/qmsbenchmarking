package core;

import utils.PropFileReader;
import utils.Utils;

import java.util.ArrayList;
import java.util.List;

import static core.BenchMarkingConstants.*;

/**
 * Created By Maharia
 */
public abstract class BaseProducer implements Producer {

    /**
     * Rate Limit for this producer
     */
    private final Long rateLimit;
    /**
     * Id of the producer
     */
    protected final int id;
    /**
     * Stats for this producer
     */
    protected final Stats stats;

    private List<Stats> accumulatedStats;

    private PropFileReader propFileReader;

    private Long startTime;

    private Long lastStatTime;

    protected long totalMessageSentCount;

    private long statsAccumulationTime;

    private boolean flag;

    public BaseProducer(int id, PropFileReader propFileReader) {
        this.id = id;
        this.propFileReader = propFileReader;
        String prefix = Utils.getNodeIdPrefix(PRODUCER_ROLE_TYPE, this.id);
        rateLimit = propFileReader.getLongValue(prefix + SEND_RATE_LIMIT);
        if (Long.compare(rateLimit, 0) < 0) {
            throw new IllegalArgumentException("Expected Rate Limit >=0");
        }
        long currentTime = Utils.getCurrentTime();
        stats = new Stats(currentTime);
        accumulatedStats = new ArrayList<>();
        totalMessageSentCount = 0L;
        lastStatTime = currentTime;
        statsAccumulationTime = propFileReader.getLongValue(prefix + STATS_ACCUMULATION_INTERVAL);
        if (statsAccumulationTime == 0) {
            throw new IllegalArgumentException("Stats accumulation time >0");
        }
        this.flag = true;
    }

    @Override
    public void run() {
        //TODO: Add How many message or how much time?
        while (flag) {
            long currentTime = Utils.getCurrentTime();
            if (Long.compare(rateLimit, 0L) != 0) {
                long delta = currentTime - startTime;
                //example: rateLimit is 5000 msg/s,
                //10 ms have elapsed(delta), we have sent 200 messages
                //the 200 msgs we have actually sent should have taken us
                //200 * 1000 / 5000 = 40 ms. So we pause for 40ms - 10ms
                long pause = (long) (totalMessageSentCount * 1000.0) / (rateLimit - delta);
                if (pause > 0) {
                    try {
                        Thread.sleep(pause);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            synchronized (this.stats) {
                if (Long.compare(currentTime - lastStatTime, statsAccumulationTime) >= 0) {
                    accumulatedStats.add(new Stats(stats, currentTime));
                    lastStatTime = currentTime;
                }
                stats.incrementSendCount();
            }
            Message message = new Message(this.id, currentTime, totalMessageSentCount);
            doProduce(message);
            totalMessageSentCount++;
        }
    }

    @Override
    public void stop() {
        this.flag = false;
        stats.setEndTime(Utils.getCurrentTime());
        accumulatedStats.add(stats);
        System.out.println("Producer: " + this.id + "Stats: ");
        System.out.println(accumulatedStats);
        doStop();
    }

    protected abstract void doStop();

    protected abstract void doProduce(Message message);
}
