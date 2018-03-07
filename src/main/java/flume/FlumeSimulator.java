package flume;

import core.BaseSimulator;
import core.Consumer;
import core.Producer;
import utils.PropFileReader;

import java.util.concurrent.atomic.AtomicLong;

import static core.BenchMarkingConstants.FLUME;
import static core.BenchMarkingConstants.IS_HOMOGENOUS_MESSAGE_SYSTEM;

/**
 * Created by Maharia
 */
public class FlumeSimulator extends BaseSimulator {

    @Override
    public String getQMSType() {
        return FLUME;
    }

    @Override
    public Producer createProducerThread(int id, PropFileReader propFileReader, AtomicLong atomicLong) {
        Boolean isHomoGenous = propFileReader.getBooleanValue(IS_HOMOGENOUS_MESSAGE_SYSTEM);
        if (isHomoGenous) {
            return new FixedLengthRpcFlumeProducer(id, propFileReader,
                    atomicLong);
        }
        return new HeterogenousRpcFlumeProducer(id, propFileReader,
                atomicLong);
    }

    @Override
    public Consumer createConsumerThread(int id, PropFileReader propFileReader) {
        return null;
    }
}
