package core;

import java.io.FileWriter;
import java.io.IOException;

public class StatsAccumulator implements Runnable {
    private FileWriter fileWriter;
    private QMSNode qmsNode;
    private final Long statsAccumulationTime;
    private String statsOutputPath;
    //    private List<Stats> accumulatedStats;
    private long lastStatUpdateTime;
    private boolean flag;

    StatsAccumulator(QMSNode qmsNode, long statsAccumulationTime, String statsOutputPath) {
        this.qmsNode = qmsNode;
        this.statsAccumulationTime = statsAccumulationTime;
        this.statsOutputPath = statsOutputPath;
//        this.accumulatedStats = new ArrayList<>();
        this.flag = true;
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
            Stats stats = qmsNode.getCurrentStatsSnapShot();
            this.lastStatUpdateTime = stats.getEndTime();
//            accumulatedStats.add(stats);
            try {
                fileWriter.write(stats.getRowValues());
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

    void stop(Stats stats) {
        //stop the thread
        if (this.flag) {
            this.flag = false;
            //handle race condition if any
            try {
                fileWriter.write(stats.getRowValues());
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
}
