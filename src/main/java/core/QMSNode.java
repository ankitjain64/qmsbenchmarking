package core;

public interface QMSNode extends Runnable {

    int getId();

    void stop();
}
