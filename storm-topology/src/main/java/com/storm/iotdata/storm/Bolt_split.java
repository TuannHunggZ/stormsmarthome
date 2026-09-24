package com.storm.iotdata.storm;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Fields;

import com.storm.iotdata.models.StormConfig;

public class Bolt_split extends BaseRichBolt {

    private static final String WINDOW_STREAM_PREFIX = "window-";
    private static final String PUNCTUATION_STREAM_PREFIX = "punctuation-";
    private transient OutputCollector collector;

    @Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

    @Override
    public void execute(Tuple tuple) {
        try {
            if (tuple.getSourceStreamId().equals("data")) {
                Integer houseId     = tuple.getIntegerByField("houseId");
                Integer householdId = tuple.getIntegerByField("householdId");
                Integer plugId      = tuple.getIntegerByField("plugId");
                Long    timestamp   = tuple.getLongByField("timestamp");
                Double  value       = tuple.getDoubleByField("value");

                ZonedDateTime dateTime = Instant.ofEpochSecond(timestamp).atZone(ZoneId.of("Europe/Berlin"));

                String year = String.valueOf(dateTime.getYear());
                String month = String.format("%02d", dateTime.getMonthValue());
                String day = String.format("%02d", dateTime.getDayOfMonth());
                Long time = (dateTime.toInstant().toEpochMilli() % 86400000);

                for (Integer window : StormConfig.getTimeSliceMinutes()) {
                    int sliceIndex = (int) Math.floorDiv(time, (window * 60000));
                    collector.emit(WINDOW_STREAM_PREFIX + window, new Values(houseId, householdId, plugId, year, month, day, sliceIndex, value));
                }
            } else if (tuple.getSourceStreamId().startsWith(PUNCTUATION_STREAM_PREFIX)) {
                int windowSize = tuple.getIntegerByField("windowSize");
                long triggerTimestampMillis = tuple.getLongByField("triggerTimestampMillis");
                collector.emit("punctuation-" + windowSize + "m", new Values(triggerTimestampMillis));
            } else {
                collector.fail(tuple);
            }
        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Integer window : StormConfig.getTimeSliceMinutes()) {
            declarer.declareStream(WINDOW_STREAM_PREFIX + window, new Fields("houseId", "householdId", "plugId", "year", "month", "day", "sliceIndex", "value"));
            declarer.declareStream(PUNCTUATION_STREAM_PREFIX + window + "m", new Fields("triggerTimestampMillis"));
        }
    }
}