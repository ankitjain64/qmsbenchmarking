package core;

import utils.PropFileReader;
import utils.Utils;

import java.util.concurrent.atomic.AtomicLong;

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
    private final Long totalMessagesToSend;

    private PropFileReader propFileReader;

    private AtomicLong atomicLong;

    private Long startTime;

    protected long totalMessageSentCount;

    private boolean flag;

    private boolean produce;

    public BaseProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) {
        this.id = id;
        this.propFileReader = propFileReader;
        String prefix = Utils.getNodeIdPrefix(PRODUCER_ROLE_TYPE, this.id);
        rateLimit = propFileReader.getLongValue(SEND_RATE_LIMIT);
        if (Long.compare(rateLimit, 0) < 0) {
            throw new IllegalArgumentException("Expected Rate Limit >=0");
        }
        startTime = Utils.getCurrentTime();
        this.stats = new Stats(startTime);
        totalMessageSentCount = 0L;
        this.totalMessagesToSend = (propFileReader.getLongValue(prefix + TOTAL_MESSAGE_TO_SEND, 100000L));
        this.atomicLong = atomicLong;
        this.flag = true;
        this.produce = true;
    }

    @Override
    public Stats getCurrentStatsSnapShot() {
        return this.stats.createSnapShot(Utils.getCurrentTime());
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
                long pause = (long) ((totalMessageSentCount * 1000.0) / (rateLimit)) - delta;
                if (pause > 0) {
                    try {
                        Thread.sleep(pause);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (this.produce) {
                long messageNumber = 0L;
                Message message = new Message(this.id, Utils.getCurrentTime(), messageNumber);
                doProduce(message);
                totalMessageSentCount++;
                updateStats();
            }

            if (totalMessageSentCount == totalMessagesToSend) {
                produce = false;
                //do not stop the accumulator
            }
            Long sendCount = stats.getSendCount();
            Long ackCount = stats.getAckCount();
            Long failedCount = stats.getFailedCount();
            if (!produce && Long.compare(sendCount, ackCount + failedCount) == 0) {
                this.stop();
            }
        }
    }

    @Override
    public void stop() {
        this.flag = false;
        doStop();
    }

    @Override
    public int getId() {
        return this.id;
    }

    private void updateStats() {
        this.stats.incrementSendCount();
    }

    protected abstract void doStop();

    protected abstract void doProduce(Message message);
}
