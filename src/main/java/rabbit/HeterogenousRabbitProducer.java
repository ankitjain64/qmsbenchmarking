package rabbit;

import utils.PropFileReader;
import utils.Utils;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class HeterogenousRabbitProducer extends BaseRabbitProducer {

    private Random random;
    private static int charSize;

    static {
        charSize = Utils.getCharByteSize();
    }

    HeterogenousRabbitProducer(int id, PropFileReader propFileReader, AtomicLong atomicLong) throws IOException, TimeoutException {
        super(id, propFileReader,atomicLong);
        random = new Random(0);
    }

    @Override
    protected String getMessageText() {
        return Utils.generateHeterogenousText(random, charSize);
    }
}
