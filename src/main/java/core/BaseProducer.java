package core;

import utils.PropFileReader;
import utils.Utils;

import static core.BenchMarkingConstants.*;
import static utils.Utils.getCurrentTime;

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
    private final StatsAccumulator statsAccumulator;
    private final Long totalMessagesToSend;

    private PropFileReader propFileReader;

    private Long startTime;

    protected long totalMessageSentCount;

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
        startTime = currentTime;
        stats = new Stats(currentTime);
        totalMessageSentCount = 0L;
        long statsAccumulationTime = propFileReader.getLongValue(prefix + STATS_ACCUMULATION_INTERVAL);
        if (statsAccumulationTime == 0) {
            throw new IllegalArgumentException("Stats accumulation time >0");
        }
        String statsOutputPath = propFileReader.getStringValue(prefix + STATS_OUTPUT_PATH);
        statsAccumulator = new StatsAccumulator(this, statsAccumulationTime, statsOutputPath);
        this.totalMessagesToSend = (propFileReader.getLongValue(prefix + TOTAL_MESSAGE_TO_SEND, 100000L));
        this.flag = true;
    }

    @Override
    public void run() {
        //TODO: Add How many message or how much time?
        new Thread(statsAccumulator).run();
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
            Message message = new Message(this.id, currentTime, totalMessageSentCount);
            doProduce(message);
            totalMessageSentCount++;
            if (totalMessageSentCount == totalMessagesToSend) {
                this.stop();
            }
        }
    }

    @Override
    public void stop() {
        if (this.flag) {
            this.flag = false;
            statsAccumulator.stop(this.getCurrentStatsSnapShot());
            doStop();
        }
    }

    @Override
    public int getId() {
        return this.id;
    }

    @Override
    public Stats getCurrentStatsSnapShot() {
        synchronized (this.stats) {
            return this.stats.createSnapShot(getCurrentTime());
        }
    }

    protected abstract void doStop();

    protected abstract void doProduce(Message message);
}
