package core;

import utils.PropFileReader;

import static core.BenchMarkingConstants.CONSUMER_ROLE_TYPE;
import static core.BenchMarkingConstants.STATS_ACCUMULATION_INTERVAL;
import static utils.Utils.getCurrentTime;

/**
 * Created By Maharia
 */
public abstract class BaseConsumer implements Consumer {

    private PropFileReader propFileReader;
    protected int id;
    protected final Stats stats;
    private final StatsAccumulator statsAccumulator;

    public BaseConsumer(int id, PropFileReader propFileReader) {
        this.id = id;
        this.propFileReader = propFileReader;
        long currentTime = getCurrentTime();
        this.stats = new Stats(currentTime);
        String prefix = CONSUMER_ROLE_TYPE + "_" + id + ".";
        long statsAccumulationTime = propFileReader.getLongValue(prefix + STATS_ACCUMULATION_INTERVAL, 0L);
        if (statsAccumulationTime == 0) {
            throw new IllegalArgumentException("Stats accumulation time >0");
        }
        statsAccumulator = new StatsAccumulator(this, statsAccumulationTime);
        new Thread(statsAccumulator).run();
    }

    protected void updateStats() {
        synchronized (this.stats) {
            stats.incrementRcvCount();
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
