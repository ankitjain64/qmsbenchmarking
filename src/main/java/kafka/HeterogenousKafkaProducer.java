package kafka;

import core.Stats;
import utils.PropFileReader;
import utils.Utils;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Maharia
 */
public class HeterogenousKafkaProducer extends BaseKafkaProducer {

    private Random random;
    private static int charSize;

    static {
        charSize = Utils.getCharByteSize();
    }

    HeterogenousKafkaProducer(int id, Stats stats, PropFileReader propFileReader, AtomicLong atomicLong) {
        super(id, stats, propFileReader, atomicLong);
        this.random = new Random(0);
    }

    @Override
    protected String getMessageText() {
        return Utils.generateHeterogenousText(random, charSize);
    }
}
