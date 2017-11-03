import core.Simulator;
import core.SimulatorRegistry;
import utils.PropFileReader;

import static core.BenchMarkingConstants.QMS_NAME;

public class AppDriver {

    public static void main(String[] args) {
        String propertiesFile = args[0];
        PropFileReader propFileReader = new PropFileReader(propertiesFile);
        String qmsName = propFileReader.getStringValue(QMS_NAME);
        Simulator simulator = SimulatorRegistry.getSimulatorInstance(qmsName);
        simulator.simulate(propFileReader);
    }
}
