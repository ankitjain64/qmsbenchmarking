package rabbit;

import core.BaseSimulator;
import core.Consumer;
import core.Producer;
import utils.PropFileReader;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static core.BenchMarkingConstants.IS_HOMOGENOUS_MESSAGE_SYSTEM;
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
    public Producer createProducerThread(int id, PropFileReader propFileReader, AtomicLong atomicLong) {
        Boolean isHomoGenous = propFileReader.getBooleanValue(IS_HOMOGENOUS_MESSAGE_SYSTEM);
        if (isHomoGenous) {
            try {
                return new FixedLengthRabbitProducer(id, propFileReader,
                        atomicLong);
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
        try {
            return new HeterogenousRabbitProducer(id, propFileReader,
                    atomicLong);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Consumer createConsumerThread(int id, PropFileReader propFileReader) {
        try {
            return new RabbitConsumer(id, propFileReader);
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        return null;
    }
}
