package kafka;

import utils.PropFileReader;
import utils.Utils;

import java.util.Random;

/**
 * Created by Maharia
 */
@SuppressWarnings("WeakerAccess")
public class HeterogenousKafkaProducer extends BaseKafkaProducer {

    private Random random;
    private static int charSize;

    static {
        charSize = Utils.getCharByteSize();
    }

    public HeterogenousKafkaProducer(int id, PropFileReader propFileReader) {
        super(id, propFileReader);
        this.random = new Random(0);
    }

    @Override
    protected String getMessageText() {
        return Utils.generateHeterogenousText(random, charSize);
    }
}
