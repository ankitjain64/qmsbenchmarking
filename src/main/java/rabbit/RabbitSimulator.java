package rabbit;

import core.BaseSimulator;
import core.Consumer;
import core.Producer;
import core.Simulator;
import utils.PropFileReader;

import static core.BenchMarkingConstants.RABBIT_MQ;

/**
 * Created by Maharia
 */
public class RabbitSimulator extends BaseSimulator {

    @Override
    public String getQMSType() {
        return RABBIT_MQ;
    }

    @Override
    public Producer createProducerThread(int id, PropFileReader propFileReader) {
        return null;
    }

    @Override
    public Consumer createConsumerThread(int id, PropFileReader propFileReader) {
        return null;
    }
}
