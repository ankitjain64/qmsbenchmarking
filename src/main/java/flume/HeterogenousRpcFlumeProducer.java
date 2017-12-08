package flume;

import core.Stats;
import utils.PropFileReader;
import utils.Utils;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class HeterogenousRpcFlumeProducer extends BaseRpcFlumeProducer {
    private Random random;
    private static int charSize;

    static {
        charSize = Utils.getCharByteSize();
    }

    public HeterogenousRpcFlumeProducer(int id, Stats stats, PropFileReader propFileReader, AtomicLong atomicLong) {
        super(id, stats, propFileReader, atomicLong);
        this.random = new Random(0);
    }

    @Override
    public String getText() {
        return Utils.generateHeterogenousText(random, charSize);
    }
}
