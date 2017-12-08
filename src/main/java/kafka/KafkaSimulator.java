package kafka;

import core.*;
import utils.PropFileReader;

import java.util.concurrent.atomic.AtomicLong;

import static core.BenchMarkingConstants.IS_HOMOGENOUS_MESSAGE_SYSTEM;

/**
 * Created by Maharia
 */
public class KafkaSimulator extends BaseSimulator {

    @Override
    public String getQMSType() {
        return BenchMarkingConstants.KAFKA;
    }

    @Override
    public Producer createProducerThread(int id, Stats stats, PropFileReader propFileReader, AtomicLong atomicLong) {
        //TODO: Fix me and change config
        Boolean isHomoGenous = propFileReader.getBooleanValue(IS_HOMOGENOUS_MESSAGE_SYSTEM);
        if (isHomoGenous) {
            return new FixedLengthMsgBaseKafkaProducer(id, stats, propFileReader,
                    atomicLong);
        }
        return new HeterogenousKafkaProducer(id, stats,propFileReader,
                atomicLong);
    }

    @Override
    public Consumer createConsumerThread(int id, PropFileReader propFileReader) {
        return new BaseKafkaConsumer(id, propFileReader);
    }
}
