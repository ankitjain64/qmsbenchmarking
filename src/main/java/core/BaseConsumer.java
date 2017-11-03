package core;

import utils.PropFileReader;
import utils.Utils;

import java.util.ArrayList;
import java.util.List;

import static core.BenchMarkingConstants.CONSUMER_ROLE_TYPE;
import static core.BenchMarkingConstants.STATS_ACCUMULATION_INTERVAL;

/**
 * Created By Maharia
 */
public abstract class BaseConsumer implements Consumer {

    private long lastStatTime;
    private final Long statsAccumulationTime;
    private PropFileReader propFileReader;
    protected int id;
    protected final Stats stats;
    private List<Stats> accumulatedStats;
    private long lastStatUpdateTime;

    public BaseConsumer(int id, PropFileReader propFileReader) {
        this.id = id;
        this.propFileReader = propFileReader;
        long currentTime = Utils.getCurrentTime();
        this.stats = new Stats(currentTime);
        lastStatTime = currentTime;
        String prefix = CONSUMER_ROLE_TYPE + "_" + id + ".";
        statsAccumulationTime = propFileReader.getLongValue(prefix + STATS_ACCUMULATION_INTERVAL);
        if (statsAccumulationTime == 0) {
            throw new IllegalArgumentException("Stats accumulation time >0");
        }
        accumulatedStats = new ArrayList<>();
    }

    protected void updateStats() {
        synchronized (this.stats) {
            stats.incrementRcvCount();
            long currentTime = Utils.getCurrentTime();
            accumulateStatsIfRequired(currentTime);
        }
    }

    private void accumulateStatsIfRequired(long currentTime) {
        synchronized (this.stats) {
            if (Long.compare(currentTime - lastStatTime, statsAccumulationTime) >= 0) {
                accumulatedStats.add(new Stats(stats, currentTime));
                lastStatTime = currentTime;
            }
            lastStatUpdateTime = currentTime;
        }
    }

    @Override
    public void stop() {
        //cant do current time since consumer will live longer and we need to
        // manually kill
        stats.setEndTime(lastStatUpdateTime);
        accumulatedStats.add(stats);
        System.out.println("Consumer: " + this.id + "Stats: ");
        System.out.println(accumulatedStats);
        doStop();
    }

    protected abstract void doStop();
}
