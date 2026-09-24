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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Storm bolt that splits load data into configured time-window streams and
 * forwards punctuation events to the matching punctuation stream.
 */
public class Bolt_split extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_split.class);

    private static final String WINDOW_STREAM_PREFIX = "window-";
    private static final String PUNCTUATION_STREAM_PREFIX = "punctuation-";
    private transient OutputCollector collector;

    /**
     * Initializes the bolt output collector.
     *
     * @param stormConf Storm configuration map.
     * @param context Topology context.
     * @param collector Storm output collector used for emitting tuples.
     */
    @Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

    /**
     * Processes data and punctuation tuples from the spout.
     * Data tuples are emitted to one stream for each configured time window.
     * Punctuation tuples are forwarded with their trigger timestamp.
     *
     * @param tuple Incoming Storm tuple.
     */
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
					LOGGER.debug("Emitted data for house={}, window={}m, sliceIndex={}", houseId, window, sliceIndex);
                }
            } else if (tuple.getSourceStreamId().startsWith(PUNCTUATION_STREAM_PREFIX)) {
                int windowSize = tuple.getIntegerByField("windowSize");
                long triggerTimestampMillis = tuple.getLongByField("triggerTimestampMillis");
                collector.emit("punctuation-" + windowSize + "m", new Values(triggerTimestampMillis));
				LOGGER.info("Forwarded punctuation: window={}m triggerTimestampMillis={}", windowSize, triggerTimestampMillis);
            } else {
				LOGGER.warn("Received tuple from unsupported stream {}", tuple.getSourceStreamId());
                collector.fail(tuple);
            }
        } catch (Exception e) {
			LOGGER.error("Failed to process tuple from stream {}", tuple.getSourceStreamId(), e);
            collector.fail(tuple);
        }
    }

    /**
     * Declares the output fields for all configured data-window and punctuation streams.
     *
     * @param declarer Storm declarer.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Integer window : StormConfig.getTimeSliceMinutes()) {
            declarer.declareStream(WINDOW_STREAM_PREFIX + window + "m", new Fields("houseId", "householdId", "plugId", "year", "month", "day", "sliceIndex", "value"));
            declarer.declareStream(PUNCTUATION_STREAM_PREFIX + window + "m", new Fields("triggerTimestampMillis"));
        }
    }
}