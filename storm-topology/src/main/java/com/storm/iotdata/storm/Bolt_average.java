package com.storm.iotdata.storm;

import com.storm.iotdata.functions.DB_store;
import com.storm.iotdata.models.PlugData;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * Bolt_average gom du lieu plug theo tung timeslice de tinh trung binh,
 * luu PostgreSQL va phat hien bat thuong theo co che rolling statistic.
 *
 * Vai tro:
 * - Nhan du lieu tu stream `window-*` cua `Bolt_split`.
 * - Tich luy `value` va `count` cho tung plug trong tung timeslice.
 * - Khi nhan punctuation, ghi du lieu vao `plug_data` va `house_data`.
 * - So sanh average hien tai voi min/max/avg lich su cua plug va house.
 *
 * Monitoring throughput va cac file log tam thoi khong duoc su dung.
 */
public class Bolt_average extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_average.class);
    private static final String WINDOW_STREAM_PREFIX = "window-";
    private static final String PUNCTUATION_STREAM_PREFIX = "punctuation-";
    private static final int DEFAULT_ANOMALY_THRESHOLD_PERCENT = 20;

    private final Integer gap;
    private final DB_store dbStore;
    private final Map<String, PlugData> plugDataList;
    private final Map<String, RollingStatistic> plugStatistics;
    private final Map<Integer, RollingStatistic> houseStatistics;
    private final int anomalyThresholdPercent;
    private transient OutputCollector collector;

    /**
     * Creates an average bolt for one configured window size.
     *
     * @param windowSizeMinutes Window size handled by this bolt, in minutes.
     */
    public Bolt_average(Integer windowSizeMinutes) {
        this.gap = windowSizeMinutes;
        this.dbStore = new DB_store();
        this.plugDataList = new HashMap<String, PlugData>();
        this.plugStatistics = new HashMap<String, RollingStatistic>();
        this.houseStatistics = new HashMap<Integer, RollingStatistic>();
        this.anomalyThresholdPercent = getIntegerSetting(
            "ANOMALY_THRESHOLD_PERCENT",
            DEFAULT_ANOMALY_THRESHOLD_PERCENT
        );
    }

    /**
     * Opens the PostgreSQL store and prepares the bolt collector.
     *
     * @param stormConf Storm configuration map.
     * @param context Topology context.
     * @param collector Storm output collector used for tuple acknowledgements.
     */
    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        dbStore.initialize();
        LOGGER.info("Bolt_Average initialized for window {}m", gap);
    }

    /**
     * Accumulates window data and flushes it when punctuation arrives.
     *
     * @param tuple Incoming Storm tuple.
     */
    @Override
    public void execute(Tuple tuple) {
        try {
            if (tuple.getSourceStreamId().equals(getPunctuationStreamId())) {
                processPunctuation(tuple);
                collector.ack(tuple);
            } else if (tuple.getSourceStreamId().equals(getWindowStreamId())) {
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

    /**
     * This bolt persists aggregates directly and does not emit another stream.
     *
     * @param declarer Storm declarer.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Aggregates and anomaly results are handled by this bolt.
    }

    /**
     * Closes the database store and clears in-memory state.
     */
    @Override
    public void cleanup() {
        dbStore.close();
        plugDataList.clear();
        plugStatistics.clear();
        houseStatistics.clear();
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

        PlugData plugData = new PlugData(
            houseId,
            householdId,
            plugId,
            year,
            month,
            day,
            sliceIndex,
            gap
        );
        String uniqueId = plugData.getUniqueId();
        plugDataList.put(
            uniqueId,
            plugDataList.getOrDefault(uniqueId, plugData).increaseValue(value)
        );
    }

    private void processPunctuation(Tuple tuple) {
        Integer punctuationWindowSize = tuple.getIntegerByField("windowSize");
        Long triggerTimestampMillis = tuple.getLongByField("triggerTimestampMillis");

        if (!gap.equals(punctuationWindowSize)) {
            LOGGER.debug("Ignoring punctuation for window {}m in bolt configured for {}m", punctuationWindowSize, gap);
            return;
        }

        if (plugDataList.isEmpty()) {
            LOGGER.debug("No accumulated data to flush for window {}m", gap);
            return;
        }

        Stack<PlugData> needSave = new Stack<PlugData>();
        needSave.addAll(plugDataList.values());

        if (!dbStore.pushPlugData(needSave)) {
            LOGGER.error("Failed to persist {} plug records for window {}m", needSave.size(), gap);
            return;
        }

        Map<Integer, HouseAggregate> houseAggregates = new HashMap<Integer, HouseAggregate>();
        for (PlugData plugData : needSave) {
            updatePlugAnomaly(plugData, triggerTimestampMillis);
            HouseAggregate houseAggregate = houseAggregates.getOrDefault(
                plugData.getHouseId(),
                new HouseAggregate()
            );
            houseAggregate.add(plugData.getAvg());
            houseAggregates.put(plugData.getHouseId(), houseAggregate);
        }
        for (Map.Entry<Integer, HouseAggregate> entry : houseAggregates.entrySet()) {
            updateHouseAnomaly(entry.getKey(), entry.getValue().average(), triggerTimestampMillis);
        }

        plugDataList.clear();
        LOGGER.info(
            "Flushed {} plug records and {} house records for window {}m",
            needSave.size(),
            houseAggregates.size(),
            gap
        );
    }

    private void updatePlugAnomaly(PlugData plugData, long triggerTimestampMillis) {
        double currentAverage = plugData.getAvg();
        if (currentAverage == 0.0d) {
            return;
        }

        String plugId = plugData.getPlugUniqueId();
        RollingStatistic statistic = plugStatistics.getOrDefault(plugId, new RollingStatistic());
        checkAnomaly(
            "PLUG",
            plugData.getHouseId(),
            plugData.getHouseholdId(),
            plugData.getPlugId(),
            currentAverage,
            statistic,
            triggerTimestampMillis
        );
        statistic.addValue(currentAverage);
        plugStatistics.put(plugId, statistic);
    }

    private void updateHouseAnomaly(int houseId, double currentAverage, long triggerTimestampMillis) {
        if (currentAverage == 0.0d) {
            return;
        }

        RollingStatistic statistic = houseStatistics.getOrDefault(houseId, new RollingStatistic());
        checkAnomaly(
            "HOUSE",
            houseId,
            null,
            null,
            currentAverage,
            statistic,
            triggerTimestampMillis
        );
        statistic.addValue(currentAverage);
        houseStatistics.put(houseId, statistic);
    }

    private void checkAnomaly(
        String entityType,
        int houseId,
        Integer householdId,
        Integer plugId,
        double currentAverage,
        RollingStatistic statistic,
        long triggerTimestampMillis
    ) {
        if (statistic.count == 0L) {
            return;
        }

        double threshold = anomalyThresholdPercent / 100.0d;
        if ((currentAverage - statistic.max) >= statistic.max * threshold) {
            logAnomaly("MAX", entityType, houseId, householdId, plugId, currentAverage, statistic, triggerTimestampMillis);
        }
        if ((currentAverage - statistic.average) >= statistic.average * threshold) {
            logAnomaly("AVG", entityType, houseId, householdId, plugId, currentAverage, statistic, triggerTimestampMillis);
        }
        if ((statistic.min - currentAverage) >= statistic.min * threshold) {
            logAnomaly("MIN", entityType, houseId, householdId, plugId, currentAverage, statistic, triggerTimestampMillis);
        }
    }

    private void logAnomaly(
        String anomalyType,
        String entityType,
        int houseId,
        Integer householdId,
        Integer plugId,
        double currentAverage,
        RollingStatistic statistic,
        long triggerTimestampMillis
    ) {
        LOGGER.warn(
            "{} anomaly detected: type={} windowSize={} houseId={} householdId={} plugId={} value={} avg={} min={} max={} triggerTimestampMillis={} thresholdPercent={}",
            entityType,
            anomalyType,
            gap,
            houseId,
            householdId,
            plugId,
            currentAverage,
            statistic.average,
            statistic.min,
            statistic.max,
            triggerTimestampMillis,
            anomalyThresholdPercent
        );
    }

    private String getWindowStreamId() {
        return WINDOW_STREAM_PREFIX + gap + "m";
    }

    private String getPunctuationStreamId() {
        return PUNCTUATION_STREAM_PREFIX + gap + "m";
    }

    private static int getIntegerSetting(String name, int defaultValue) {
        String value = System.getProperty(name);
        if (value == null || value.trim().isEmpty()) {
            value = System.getenv(name);
        }
        try {
            return value == null ? defaultValue : Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            return defaultValue;
        }
    }

    private static final class HouseAggregate {

        private double total;
        private int count;

        private void add(double value) {
            total += value;
            count += 1;
        }

        private double average() {
            return count == 0 ? 0.0d : total / count;
        }
    }

    private static final class RollingStatistic {

        private double min;
        private double max;
        private double average;
        private long count;

        private void addValue(double value) {
            if (value == 0.0d) {
                return;
            }
            if (count == 0L) {
                min = value;
                max = value;
                average = value;
                count = 1L;
                return;
            }
            long updatedCount = count + 1L;
            average = (average * count + value) / updatedCount;
            count = updatedCount;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
    }
}
