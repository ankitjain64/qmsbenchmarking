package core;

import utils.PropFileReader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static core.BenchMarkingConstants.*;

/**
 * Created by Maharia
 */
public abstract class BaseSimulator implements Simulator {

    @Override
    public void simulate(PropFileReader propFileReader) {
        String roleType = propFileReader.getStringValue(NODE_ROLE);
        Integer nodeCount = propFileReader.getIntegerValue(NODE_COUNT);
        final ExecutorService executorService = Executors.newFixedThreadPool(nodeCount);
        final List<QMSNode> qmsNodes = new ArrayList<>();
        if (PRODUCER_ROLE_TYPE.equals(roleType)) {
            for (int i = 0; i < nodeCount; i++) {
                qmsNodes.add(createProducerThread(i, propFileReader));
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

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (QMSNode qmsNode : qmsNodes) {
                    qmsNode.stop();
                }
                executorService.shutdown();
                try {
                    executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
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
     * @param propFileReader prop file read to read properties while creating
     *                       the instance
     * @return Producer instance
     */
    public abstract Producer createProducerThread(int id, PropFileReader propFileReader);

    public abstract Consumer createConsumerThread(int id, PropFileReader propFileReader);

}
