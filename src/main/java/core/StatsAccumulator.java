package core;

import java.util.ArrayList;
import java.util.List;

public class StatsAccumulator implements Runnable {
    private QMSNode qmsNode;
    private final Long statsAccumulationTime;
    private List<Stats> accumulatedStats;
    private long lastStatUpdateTime;
    private boolean flag;

    StatsAccumulator(QMSNode qmsNode, long statsAccumulationTime) {
        this.qmsNode = qmsNode;
        this.statsAccumulationTime = statsAccumulationTime;
        this.accumulatedStats = new ArrayList<>();
        this.flag = true;
    }


    @Override
    public void run() {
        while (flag) {
            Stats stats = qmsNode.getCurrentStatsSnapShot();
            this.lastStatUpdateTime = stats.getEndTime();
            accumulatedStats.add(stats);
            try {
                Thread.sleep(statsAccumulationTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop(Stats stats) {
        this.flag = false;
        stats.setEndTime(lastStatUpdateTime);
        accumulatedStats.add(stats);
        System.out.println("Consumer: " + qmsNode.getId() + " has Stats: ");
        System.out.println(accumulatedStats);
    }
}
