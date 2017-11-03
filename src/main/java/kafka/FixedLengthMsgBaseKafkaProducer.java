package kafka;

import utils.PropFileReader;
import utils.Utils;

import static core.BenchMarkingConstants.MESSAGE_SIZE;
import static core.BenchMarkingConstants.PRODUCER_ROLE_TYPE;

/**
 * Created by Maharia
 */
@SuppressWarnings({"FieldCanBeLocal", "WeakerAccess"})
public class FixedLengthMsgBaseKafkaProducer extends BaseKafkaProducer {

    private int messageSize;
    private static int charSize;
    private String message;

    static {
        charSize = Utils.getCharByteSize();
    }

    public FixedLengthMsgBaseKafkaProducer(int id, PropFileReader propFileReader) {
        //TODO: Fix this
        super(id, propFileReader);
        String nodeIdPrefix = Utils.getNodeIdPrefix(PRODUCER_ROLE_TYPE, this.id);
        this.messageSize = propFileReader.getIntegerValue(nodeIdPrefix + MESSAGE_SIZE);
        this.message = Utils.generateMessageText(messageSize, charSize);
    }

    @Override
    protected String getMessageText() {
        return message;
    }
}
