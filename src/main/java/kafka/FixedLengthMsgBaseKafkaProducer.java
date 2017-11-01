package kafka;

import utils.PropFileReader;
import utils.Utils;

import static core.BenchMarkingConstants.MESSAGE_SIZE;

/**
 * Created by Maharia
 */
@SuppressWarnings("FieldCanBeLocal")
public class FixedLengthMsgBaseKafkaProducer extends BaseKafkaProducer {

    private int messageSize;
    private int charSize;
    private String message;

    public FixedLengthMsgBaseKafkaProducer(int id, PropFileReader propFileReader) {
        //TODO: Fix this
        super(id, propFileReader);
        this.messageSize = propFileReader.getIntegerValue(MESSAGE_SIZE);
        this.charSize = Utils.getCharByteSize();
        int numberOfChars = messageSize / charSize;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numberOfChars; i++) {
            sb.append('a');
        }
        this.message = sb.toString();
    }

    @Override
    protected String getMessage() {
        return message;
    }
}
