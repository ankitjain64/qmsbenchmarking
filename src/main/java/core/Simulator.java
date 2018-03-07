package core;

import utils.PropFileReader;

/**
 * Created By Maharia
 */
public interface Simulator {
    String getQMSType();

    void simulate(PropFileReader propFileReader);

    Stats getCurrentStatsSnapShot();
}
