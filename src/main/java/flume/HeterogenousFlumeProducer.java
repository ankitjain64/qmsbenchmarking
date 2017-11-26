package flume;

import utils.PropFileReader;
import utils.Utils;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class HeterogenousFlumeProducer extends BaseFlumeProducer {
    private Random random;
    private static int charSize;

    static {
        charSize = Utils.getCharByteSize();
    }

    public HeterogenousFlumeProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) {
        super(id, propFileReader, atomicLong);
        this.random = new Random(0);
    }

    @Override
    public String getText() {
        return Utils.generateHeterogenousText(random, charSize);
    }
}
