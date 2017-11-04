package kafka;

import utils.PropFileReader;
import utils.Utils;

import java.util.concurrent.atomic.AtomicLong;

import static core.BenchMarkingConstants.MESSAGE_SIZE;
import static core.BenchMarkingConstants.PRODUCER_ROLE_TYPE;

/**
 * Created by Maharia
 */
public class FixedLengthMsgBaseKafkaProducer extends BaseKafkaProducer {

    private static int charSize;
    private String message;

    static {
        charSize = Utils.getCharByteSize();
    }

    FixedLengthMsgBaseKafkaProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) {
        //TODO: Fix this
        super(id, propFileReader, atomicLong);
        String nodeIdPrefix = Utils.getNodeIdPrefix(PRODUCER_ROLE_TYPE, this.id);
        int messageSize = propFileReader.getIntegerValue(nodeIdPrefix + MESSAGE_SIZE);
        this.message = Utils.generateMessageText(messageSize, charSize);
    }

    @Override
    protected String getMessageText() {
        return message;
    }
}
