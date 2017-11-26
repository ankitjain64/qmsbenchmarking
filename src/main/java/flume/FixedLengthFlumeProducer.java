package flume;

import utils.PropFileReader;
import utils.Utils;

import java.util.concurrent.atomic.AtomicLong;

import static core.BenchMarkingConstants.MESSAGE_SIZE;
import static core.BenchMarkingConstants.PRODUCER_ROLE_TYPE;

public class FixedLengthFlumeProducer extends BaseFlumeProducer {

    private static int charSize;
    private String message;

    static {
        charSize = Utils.getCharByteSize();
    }

    public FixedLengthFlumeProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) {
        super(id, propFileReader, atomicLong);
        String nodeIdPrefix = Utils.getNodeIdPrefix(PRODUCER_ROLE_TYPE, this.id);
        int messageSize = propFileReader.getIntegerValue(nodeIdPrefix + MESSAGE_SIZE);
        this.message = Utils.generateMessageText(messageSize, charSize);
    }

    @Override
    public String getText() {
        return message;
    }
}
