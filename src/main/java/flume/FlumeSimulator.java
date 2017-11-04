package flume;

import core.BaseSimulator;
import core.Consumer;
import core.Producer;
import utils.PropFileReader;

import java.util.concurrent.atomic.AtomicLong;

import static core.BenchMarkingConstants.FLUME;

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
        return null;
    }

    @Override
    public Consumer createConsumerThread(int id, PropFileReader propFileReader) {
        return null;
    }
}
