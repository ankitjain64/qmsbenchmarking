package core;

import utils.PropFileReader;
import utils.Utils;

import java.util.concurrent.atomic.AtomicLong;

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
    private final Thread statsAccumulatorThread;
    private final Long totalMessagesToSend;
    private final StatsAccumulator statsAccumulator;

    private PropFileReader propFileReader;

    private AtomicLong atomicLong;

    private Long startTime;

    protected long totalMessageSentCount;

    private boolean flag;

    public BaseProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) {
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
        statsAccumulatorThread = new Thread(statsAccumulator);
        this.totalMessagesToSend = (propFileReader.getLongValue(prefix + TOTAL_MESSAGE_TO_SEND, 100000L));
        this.atomicLong = atomicLong;
        this.flag = true;
    }

    @Override
    public void run() {
        //TODO: Add How many message or how much time?
        statsAccumulatorThread.start();
        while (flag) {
            long currentTime = Utils.getCurrentTime();
            if (Long.compare(rateLimit, 0L) != 0) {
                long delta = currentTime - startTime;
                //example: rateLimit is 5000 msg/s,
                //10 ms have elapsed(delta), we have sent 200 messages
                //the 200 msgs we have actually sent should have taken us
                //200 * 1000 / 5000 = 40 ms. So we pause for 40ms - 10ms
                long pause = (long) ((totalMessageSentCount * 1000.0) / (rateLimit)) - delta;
                if (pause > 0) {
                    try {
                        Thread.sleep(pause);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            long messageNumber = atomicLong.getAndIncrement();
            Message message = new Message(this.id, Utils.getCurrentTime(), messageNumber);
            doProduce(message);
            totalMessageSentCount++;
            updateStats();
            if (totalMessageSentCount == totalMessagesToSend) {
                flag = false;
                //do not stop the accumulator
            }
        }
    }

    @Override
    public void stop() {
        this.flag = false;
        this.statsAccumulator.stop(stats);
        doStop();
    }

    @Override
    public int getId() {
        return this.id;
    }

    private void updateStats() {
        this.stats.incrementSendCount();
    }

    @Override
    public Stats getCurrentStatsSnapShot() {
        return this.stats.createSnapShot(getCurrentTime());
    }

    protected abstract void doStop();

    protected abstract void doProduce(Message message);
}
