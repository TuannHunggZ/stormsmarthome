package com.storm.iotdata;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import com.storm.iotdata.models.PlugData;
import com.storm.iotdata.models.StormConfig;
import com.storm.iotdata.storm.Bolt_average;
import com.storm.iotdata.storm.Bolt_split;
import com.storm.iotdata.storm.Spout_data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds and submits the MQTT to PostgreSQL Storm topology.
 */
public class MainTopo {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainTopo.class);

    /**
     * Creates the spout, split bolt and one average bolt per configured window.
     *
     * @param args Command-line arguments.
     */
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout-data", new Spout_data(), 1);

        BoltDeclarer splitBolt = builder.setBolt("bolt-split", new Bolt_split(), 1);
        splitBolt.shuffleGrouping("spout-data", "data");

        for (Integer windowSize : StormConfig.getTimeSliceMinutes()) {
            splitBolt.shuffleGrouping("spout-data", "punctuation-" + windowSize + "m");

            String boltId = "bolt-average-" + windowSize + "m";
            BoltDeclarer boltDeclarer = builder.setBolt(boltId, new Bolt_average(windowSize), 1);
            boltDeclarer.shuffleGrouping("bolt-split", "window-" + windowSize + "m");
            boltDeclarer.shuffleGrouping("bolt-split", "punctuation-" + windowSize + "m");
        }

        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(4);
        config.registerSerialization(PlugData.class);

        LOGGER.info("Submitting iot-smarthome topology with windows {}", StormConfig.getTimeSliceMinutes());
        StormSubmitter.submitTopology("iot-smarthome", config, builder.createTopology());
    }
}
