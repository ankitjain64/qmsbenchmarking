package kafka;

import core.Message;
import utils.PropFileReader;

public class HeterogenousKafkaProducer extends BaseKafkaProducer {

    public HeterogenousKafkaProducer(int id, PropFileReader propFileReader) {
        super(id, propFileReader);
    }

    @Override
    protected String getMessage() {
        throw new RuntimeException("Not yet implemented!!");
    }
}
