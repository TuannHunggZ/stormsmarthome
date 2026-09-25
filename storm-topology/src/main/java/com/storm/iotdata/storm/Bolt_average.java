package com.storm.iotdata.storm;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.iotdata.functions.DB_store;
import com.storm.iotdata.models.PlugData;

/**
 * Storm bolt that aggregates plug data by window and time slice.
 * Completed aggregates are emitted to the data stream and persisted by
 * DB_store when a punctuation event closes the current window.
 */
public class Bolt_average extends BaseRichBolt {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_average.class);

    private final Integer windowSizeMinutes;
    private final Map<String, PlugData> plugDataList;
    private transient OutputCollector collector;

    /**
     * Creates an average bolt for one configured window size.
     *
     * @param windowSizeMinutes Window size handled by this bolt, in minutes.
     */
    public Bolt_average(int windowSizeMinutes) {
        this.windowSizeMinutes = windowSizeMinutes;
        this.plugDataList = new HashMap<String, PlugData>();
    }

    @Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		LOGGER.info("Bolt_Average initialized for window {}m", windowSizeMinutes);
	}

    /**
     * Processes a punctuation or window-data tuple.
     *
     * @param tuple Incoming Storm tuple.
     */
    @Override
    public void execute(Tuple tuple) {
        try {
            if (tuple.getSourceStreamId().equals("punctuation-" + windowSizeMinutes + "m")) {
                processPunctuation(tuple);
                collector.ack(tuple);
            } else if (tuple.getSourceStreamId().equals("window-" + windowSizeMinutes + "m")) {
                processWindowData(tuple);
                collector.ack(tuple);
            } else {
                LOGGER.warn("Received tuple from unsupported stream {}", tuple.getSourceStreamId());
                collector.fail(tuple);
            }
        } catch (Exception exception) {
            LOGGER.error("Failed to process tuple from stream {}", tuple.getSourceStreamId(), exception);
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("data", new Fields("type", "data"));
        declarer.declareStream("punctuation-" + windowSizeMinutes + "m", new Fields("triggerTimestampMillis"));
    }

    @Override
    public void cleanup() {
		LOGGER.info("Cleaning up Bolt_Average for window {}m", windowSizeMinutes);
        plugDataList.clear();
    }

    private void processWindowData(Tuple tuple) {
        Integer houseId = tuple.getIntegerByField("houseId");
        Integer householdId = tuple.getIntegerByField("householdId");
        Integer plugId = tuple.getIntegerByField("plugId");
        String year = tuple.getStringByField("year");
        String month = tuple.getStringByField("month");
        String day = tuple.getStringByField("day");
        Integer sliceIndex = tuple.getIntegerByField("sliceIndex");
        Double value = tuple.getDoubleByField("value");

        PlugData plugData = new PlugData(houseId, householdId, plugId, year, month, day, sliceIndex, windowSizeMinutes);
        String uniqueId = plugData.getUniqueId();
        plugDataList.put(
            uniqueId,
            plugDataList.getOrDefault(uniqueId, plugData).increaseValue(value)
        );
    }

    private void processPunctuation(Tuple tuple) {
        int punctuationWindowSize = tuple.getIntegerByField("windowSize");
        long triggerTimestampMillis = tuple.getLongByField("triggerTimestampMillis");

        if (!windowSizeMinutes.equals(punctuationWindowSize)) {
            LOGGER.debug("Ignoring punctuation for window {}m in bolt configured for {}m", punctuationWindowSize, windowSizeMinutes);
            return;
        }

        Stack<PlugData> needSave = new Stack<PlugData>();
        Stack<String> needClean = new Stack<String>();

        for (String key : plugDataList.keySet()) {
            PlugData data = plugDataList.get(key);
            if (!data.isSaved()) {
                collector.emit("data", tuple, new Values(data.getClass().getSimpleName(), data));
                needSave.push(data);
            } else if (data.isSaved()) {
                needClean.push(key);
            }
        }

        if (DB_store.pushPlugData(needSave, new File("./tmp/plugData2db-" + windowSizeMinutes + ".lck"))) {
            for (PlugData plugData : needSave) {
                plugDataList.get(plugData.getUniqueId()).save();
            }
        }

        for (String key : needClean) {
            plugDataList.remove(key);
        }

        collector.emit("punctuation-" + windowSizeMinutes + "m", tuple, new Values(triggerTimestampMillis));
		LOGGER.info("Forwarded punctuation for window {}m", windowSizeMinutes);
    }
}