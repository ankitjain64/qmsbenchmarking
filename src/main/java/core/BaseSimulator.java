package core;

import utils.PropFileReader;
import utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static core.BenchMarkingConstants.*;
import static utils.Utils.getCurrentTime;

/**
 * Created by Maharia
 */
public abstract class BaseSimulator implements Simulator {

    private AtomicLong atomicLong;
    private Stats stats;

    public BaseSimulator() {
        atomicLong = new AtomicLong(0);
    }

    @Override
    public void simulate(PropFileReader propFileReader) {
        String roleType = propFileReader.getStringValue(NODE_ROLE);
        Integer nodeCount = propFileReader.getIntegerValue(NODE_COUNT);
        final ExecutorService executorService = Executors.newFixedThreadPool(nodeCount + 1);
        stats = new Stats(Utils.getCurrentTime());
        final List<QMSNode> qmsNodes = new ArrayList<>();
        long statsAccumulationTime = propFileReader.getLongValue(STATS_ACCUMULATION_INTERVAL);
        if (statsAccumulationTime == 0) {
            throw new IllegalArgumentException("Stats accumulation time >0");
        }
        String statsOutputPath = propFileReader.getStringValue(STATS_OUTPUT_PATH);
        StatsAccumulator statsAccumulator = new StatsAccumulator(this, statsAccumulationTime, statsOutputPath);
        if (PRODUCER_ROLE_TYPE.equals(roleType)) {
            for (int i = 0; i < nodeCount; i++) {
                qmsNodes.add(createProducerThread(i, stats, propFileReader, atomicLong));
            }
        } else if (CONSUMER_ROLE_TYPE.equals(roleType)) {
            for (int i = 0; i < nodeCount; i++) {
                qmsNodes.add(createConsumerThread(i, propFileReader));
            }
        } else {
            throw new IllegalArgumentException("Undefined role type: " + roleType);
        }
        for (QMSNode qmsNode : qmsNodes) {
            executorService.submit(qmsNode);
        }
//        System.out.println("Available Processors: ");
//        System.out.println(Runtime.getRuntime().availableProcessors());
        executorService.submit(statsAccumulator);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (QMSNode qmsNode : qmsNodes) {
                    qmsNode.stop();
                }
                executorService.shutdown();
                try {
                    executorService.awaitTermination(10000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public Stats getCurrentStatsSnapShot() {
        return this.stats.createSnapShot(getCurrentTime());
    }

    /**
     * TODO: Change id from int to string depending framework...lets see
     * If any specific key is to be read Read format should be:
     * key_id = value
     * For example lets say heartbeat property needs to be readed for
     * producer 0 and producer 1
     * they should be specified as heartbeat_0=12 and heartbeat_1=12
     *
     * @param id             id of the producer
     * @param stats
     * @param propFileReader prop file read to read properties while creating
     *                       the instance
     * @param atomicLong     @return Producer instance
     */
    public abstract Producer createProducerThread(int id, Stats stats, PropFileReader propFileReader, AtomicLong atomicLong);

    public abstract Consumer createConsumerThread(int id, PropFileReader propFileReader);

}
