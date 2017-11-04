package kafka;

import core.BaseSimulator;
import core.BenchMarkingConstants;
import core.Consumer;
import core.Producer;
import utils.PropFileReader;

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
    public Producer createProducerThread(int id, PropFileReader propFileReader) {
        //TODO: Fix me and change config
        Boolean isHomoGenous = propFileReader.getBooleanValue(IS_HOMOGENOUS_MESSAGE_SYSTEM);
        if (isHomoGenous) {
            return new FixedLengthMsgBaseKafkaProducer(id,propFileReader);
        }
        return new HeterogenousKafkaProducer(id,propFileReader);
    }

    @Override
    public Consumer createConsumerThread(int id, PropFileReader propFileReader) {
        return new BaseKafkaConsumer(id, propFileReader);
    }
}
