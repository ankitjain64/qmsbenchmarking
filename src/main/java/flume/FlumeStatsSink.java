package flume;

import core.Message;
import core.QMSNode;
import core.Stats;
import core.StatsAccumulator;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import utils.PropFileReader;
import utils.Utils;

import static core.BenchMarkingConstants.STATS_ACCUMULATION_INTERVAL;
import static core.BenchMarkingConstants.STATS_OUTPUT_PATH;
import static utils.Utils.getCurrentTime;

public class FlumeStatsSink extends AbstractSink implements Configurable, QMSNode {

    private StatsAccumulator statsAccumulator;
    private Stats stats = new Stats(getCurrentTime());

    @Override
    public int getId() {
        //no-op
        return 0;
    }

    @Override
    public void run() {
        //no-op
    }

    @Override
    public synchronized void start() {
        super.start();
        new Thread(statsAccumulator).start();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        statsAccumulator.stop(stats);
    }

    @Override
    public Stats getCurrentStatsSnapShot() {
        return this.stats.createSnapShot(getCurrentTime());
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status rv;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event event = channel.take();
            Message message = Utils.fromJson(event.getBody(), Message.class);
            message.setcTs(Utils.getCurrentTime());
            stats.incrementRcvCountAndLatency(message.getDelta());
            transaction.commit();
            rv = Status.READY;
        } catch (Throwable th) {
            transaction.rollback();
            rv = Status.BACKOFF;
            if (th instanceof Error) {
                throw (Error) th;
            }
        }
        return rv;
    }

    @Override
    public void configure(Context context) {
        String propFilePath = context.getString("propFilePath");
        PropFileReader propFileReader = new PropFileReader(propFilePath);
        String statsOutputPath = propFileReader.getStringValue(STATS_OUTPUT_PATH);
        long statsAccumulationTime = propFileReader.getLongValue(STATS_ACCUMULATION_INTERVAL, 0L);
        if (statsAccumulationTime == 0) {
            throw new RuntimeException("No stats accumulation interval " +
                    "provided");
        }
        statsAccumulator = new StatsAccumulator(this, statsAccumulationTime, statsOutputPath);
    }
}
