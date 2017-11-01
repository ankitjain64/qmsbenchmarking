package core;

import flume.FlumeSimulator;
import kafka.KafkaSimulator;
import rabbit.RabbitSimulator;

import java.util.HashMap;
import java.util.Map;

public class SimulatorRegistry {

    private static Map<String, Simulator> simulatorTypeVsSimulator = new HashMap<>();

    static {
        simulatorTypeVsSimulator.put(BenchMarkingConstants.KAFKA, new KafkaSimulator());
        simulatorTypeVsSimulator.put(BenchMarkingConstants.RABBIT_MQ, new RabbitSimulator());
        simulatorTypeVsSimulator.put(BenchMarkingConstants.FLUME, new FlumeSimulator());
    }

    public static Simulator getSimulatorInstance(String qmsName) {
        Simulator simulator = simulatorTypeVsSimulator.get(qmsName);
        if (simulator == null) {
            throw new IllegalArgumentException("Unknown qms type: " + qmsName);
        }
        return simulator;
    }
}
