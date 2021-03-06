package flume;

import core.Message;
import core.Simulator;
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

public class FlumeStatsSink extends AbstractSink implements Configurable, Simulator {

    private StatsAccumulator statsAccumulator;
    private Stats stats = new Stats(getCurrentTime());
    private Message lastMessage;

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
    public Status process() throws EventDeliveryException {
        Status rv;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event event = channel.take();
            if (event != null) {
                Message message = Utils.fromJson(event.getBody(), Message.class);
                message.setcTs(Utils.getCurrentTime());
                boolean isGlobalOutOfOrder = false;
                if (lastMessage != null) {
                    isGlobalOutOfOrder = Long.compare(message.getNum(), lastMessage.getNum()) < 0;
                }
                lastMessage = message;
                stats.incrementRcvCountAndLatency(message.getDelta());
                stats.setGlobalOutOfOrder(isGlobalOutOfOrder);
                rv = Status.READY;
            } else {
                rv = Status.BACKOFF;
            }
            transaction.commit();
        } catch (Throwable th) {
            transaction.rollback();
            rv = Status.BACKOFF;
            if (th instanceof Error) {
                throw (Error) th;
            }
        } finally {
            transaction.close();
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

    @Override
    public String getQMSType() {
        return null;
    }

    @Override
    public void simulate(PropFileReader propFileReader) {

    }

    @Override
    public Stats getCurrentStatsSnapShot() {
        return this.stats.createSnapShot(Utils.getCurrentTime());
    }
}
