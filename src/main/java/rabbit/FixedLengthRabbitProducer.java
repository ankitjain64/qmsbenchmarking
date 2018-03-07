package rabbit;

import utils.PropFileReader;
import utils.Utils;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static core.BenchMarkingConstants.MESSAGE_SIZE;
import static core.BenchMarkingConstants.PRODUCER_ROLE_TYPE;

public class FixedLengthRabbitProducer extends BaseRabbitProducer {

    private int messageSize;
    private static int charSize;
    private String message;

    static {
        charSize = Utils.getCharByteSize();
    }

    FixedLengthRabbitProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) throws IOException, TimeoutException {
        super(id, propFileReader, atomicLong);
        String nodeIdPrefix = Utils.getNodeIdPrefix(PRODUCER_ROLE_TYPE, this.id);
        this.messageSize = propFileReader.getIntegerValue(nodeIdPrefix + MESSAGE_SIZE);
        message = Utils.generateMessageText(messageSize, charSize);
    }

    @Override
    protected String getMessageText() {
        return message;
    }
}
