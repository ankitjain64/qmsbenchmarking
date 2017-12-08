package core;

import java.io.FileWriter;
import java.io.IOException;

public class StatsAccumulator implements Runnable {
    private FileWriter fileWriter;
    private Simulator simulator;
    private final Long statsAccumulationTime;
    private final String statsOutputPath;
    //    private List<Stats> accumulatedStats;
//    private long lastStatUpdateTime;
    private boolean flag;
    private Stats previousStats;

    public StatsAccumulator(Simulator simulator, long statsAccumulationTime,
                            String statsOutputPath) {
        this.simulator = simulator;
        this.statsAccumulationTime = statsAccumulationTime;
        this.statsOutputPath = statsOutputPath;
//        this.accumulatedStats = new ArrayList<>();
        this.flag = true;
        this.previousStats = null;
        init();
    }

    private void init() {
        try {
            fileWriter = new FileWriter(this.statsOutputPath);
            fileWriter.write(Stats.getCsvHeaders());
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Override
    public void run() {
        while (flag) {
            Stats stats = simulator.getCurrentStatsSnapShot();
            Stats currentOutput = stats.getDeltaStats(previousStats);
            previousStats = stats;
//            this.lastStatUpdateTime = stats.getEndTime();
//            accumulatedStats.add(stats);
            try {
                Long cummulativeCount = getCummulative(stats);
                fileWriter.write(currentOutput.getRowValues(cummulativeCount));
                fileWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            try {
                Thread.sleep(statsAccumulationTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void stop(Stats stats) {
        //stop the thread
        if (this.flag) {
            this.flag = false;
            Stats deltaStats = stats.getDeltaStats(previousStats);
            //handle race condition if any
            try {
                fileWriter.write(deltaStats.getRowValues(getCummulative(stats)));
                fileWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
//        accumulatedStats.add(stats);
        }
    }

    private Long getCummulative(Stats stats) {
        Long cummulativeCount = stats.getSendCount();
        if (cummulativeCount == null || cummulativeCount == 0) {
            cummulativeCount = stats.getRcvCount();
        }
        return cummulativeCount;
    }
}
