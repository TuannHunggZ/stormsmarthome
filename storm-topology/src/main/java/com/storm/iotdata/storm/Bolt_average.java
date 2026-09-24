package com.storm.iotdata.storm;

import com.storm.iotdata.models.PlugData;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bolt that aggregates plug values for one configured window size, persists
 * completed slices in PostgreSQL, and detects plug and house anomalies.
 *
 * Data is accumulated from the matching window stream. A punctuation event
 * closes the currently accumulated slices and triggers persistence and anomaly
 * detection. Monitoring and throughput reporting are intentionally excluded.
 */
public class Bolt_average extends BaseRichBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_average.class);

    private static final String DATA_STREAM_PREFIX = "window-";
    private static final String PUNCTUATION_STREAM_PREFIX = "punctuation-";
    private static final String JDBC_URL = "JDBC_URL";
    private static final String JDBC_USER = "JDBC_USER";
    private static final String JDBC_PASSWORD = "JDBC_PASSWORD";
    private static final String ANOMALY_THRESHOLD_PERCENT = "ANOMALY_THRESHOLD_PERCENT";
    private static final String DEFAULT_JDBC_URL = "jdbc:postgresql://postgresql:5432/iotdata";
    private static final String DEFAULT_JDBC_USER = "postgres";
    private static final String DEFAULT_JDBC_PASSWORD = "postgres";
    private static final int DEFAULT_ANOMALY_THRESHOLD_PERCENT = 20;

    private final int windowSizeMinutes;
    private final Map<String, PlugData> plugDataBySlice;
    private final Map<PlugKey, RollingStatistic> plugStatistics;
    private final Map<HouseKey, RollingStatistic> houseStatistics;
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final int anomalyThresholdPercent;
    private transient OutputCollector collector;
    private transient Connection connection;
    private transient PreparedStatement plugInsertStatement;
    private transient PreparedStatement houseInsertStatement;

    /**
     * Creates an average bolt for one window size.
     *
     * @param windowSizeMinutes Window size handled by this bolt, in minutes.
     */
    public Bolt_average(int windowSizeMinutes) {
        this.windowSizeMinutes = windowSizeMinutes;
        this.plugDataBySlice = new HashMap<String, PlugData>();
        this.plugStatistics = new HashMap<PlugKey, RollingStatistic>();
        this.houseStatistics = new HashMap<HouseKey, RollingStatistic>();
        this.jdbcUrl = getSetting(JDBC_URL, DEFAULT_JDBC_URL);
        this.jdbcUser = getSetting(JDBC_USER, DEFAULT_JDBC_USER);
        this.jdbcPassword = getSetting(JDBC_PASSWORD, DEFAULT_JDBC_PASSWORD);
        this.anomalyThresholdPercent = getIntegerSetting(
            ANOMALY_THRESHOLD_PERCENT,
            DEFAULT_ANOMALY_THRESHOLD_PERCENT
        );
    }

    /**
     * Initializes the PostgreSQL connection and prepared statements.
     *
     * @param stormConf Storm configuration map.
     * @param context Topology context.
     * @param collector Storm output collector used for tuple acknowledgements.
     */
    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        initializeDatabase();
        LOGGER.info("Bolt_Average initialized for window {}m", windowSizeMinutes);
    }

    /**
     * Routes data tuples to the accumulator and punctuation tuples to the flush path.
     *
     * @param tuple Incoming Storm tuple.
     */
    @Override
    public void execute(Tuple tuple) {
        String sourceStreamId = tuple.getSourceStreamId();

        try {
            if (getDataStreamId().equals(sourceStreamId)) {
                processData(tuple);
                collector.ack(tuple);
            } else if (getPunctuationStreamId().equals(sourceStreamId)) {
                processPunctuation(tuple);
                collector.ack(tuple);
            } else {
                LOGGER.warn("Received tuple from unsupported stream {}", sourceStreamId);
                collector.fail(tuple);
            }
        } catch (Exception exception) {
            LOGGER.error("Failed to process tuple from stream {}", sourceStreamId, exception);
            rollbackDatabase(exception);
            collector.fail(tuple);
        }
    }

    /**
     * This bolt is a persistence sink and does not declare output streams.
     *
     * @param declarer Storm declarer.
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Persistence and anomaly detection are completed inside this bolt.
    }

    /**
     * Flushes pending resources and clears in-memory state during shutdown.
     */
    @Override
    public void cleanup() {
        closeDatabase();
        plugDataBySlice.clear();
        plugStatistics.clear();
        houseStatistics.clear();
    }

    private void processData(Tuple tuple) {
        int houseId = tuple.getIntegerByField("houseId");
        int householdId = tuple.getIntegerByField("householdId");
        int plugId = tuple.getIntegerByField("plugId");
        String year = tuple.getStringByField("year");
        String month = tuple.getStringByField("month");
        String day = tuple.getStringByField("day");
        int sliceIndex = tuple.getIntegerByField("sliceIndex");
        double value = tuple.getDoubleByField("value");

        PlugData plugData = new PlugData(
            houseId,
            householdId,
            plugId,
            year,
            month,
            day,
            sliceIndex,
            windowSizeMinutes
        );
        String uniqueId = plugData.getUniqueId();
        PlugData accumulatedData = plugDataBySlice.getOrDefault(uniqueId, plugData);
        plugDataBySlice.put(uniqueId, accumulatedData.increaseValue(value));
    }

    private void processPunctuation(Tuple tuple) throws SQLException {
        int punctuationWindowSize = tuple.getIntegerByField("windowSize");
        long triggerTimestampMillis = tuple.getLongByField("triggerTimestampMillis");

        if (punctuationWindowSize != windowSizeMinutes) {
            LOGGER.debug(
                "Ignoring punctuation for window {}m in bolt configured for {}m",
                punctuationWindowSize,
                windowSizeMinutes
            );
            return;
        }

        if (plugDataBySlice.isEmpty()) {
            LOGGER.debug("No accumulated data to flush for window {}m", windowSizeMinutes);
            return;
        }

        List<PlugData> completedPlugData = new ArrayList<PlugData>(plugDataBySlice.values());
        Map<HouseSliceKey, HouseAccumulator> houseAggregates = buildHouseAggregates(completedPlugData);

        persistPlugData(completedPlugData);
        persistHouseData(houseAggregates);
        connection.commit();

        for (PlugData plugData : completedPlugData) {
            updatePlugAnomalyStatistics(plugData, triggerTimestampMillis);
        }
        for (Map.Entry<HouseSliceKey, HouseAccumulator> entry : houseAggregates.entrySet()) {
            updateHouseAnomalyStatistics(entry.getKey(), entry.getValue().average(), triggerTimestampMillis);
        }

        plugDataBySlice.clear();
        LOGGER.info(
            "Flushed {} plug records and {} house records for window {}m",
            completedPlugData.size(),
            houseAggregates.size(),
            windowSizeMinutes
        );
    }

    private Map<HouseSliceKey, HouseAccumulator> buildHouseAggregates(List<PlugData> plugDataList) {
        Map<HouseSliceKey, HouseAccumulator> houseAggregates = new HashMap<HouseSliceKey, HouseAccumulator>();

        for (PlugData plugData : plugDataList) {
            HouseSliceKey key = new HouseSliceKey(
                plugData.getHouseId(),
                plugData.getYear(),
                plugData.getMonth(),
                plugData.getDay(),
                plugData.getSliceIndex(),
                plugData.getSliceGap()
            );
            HouseAccumulator accumulator = houseAggregates.computeIfAbsent(
                key,
                ignored -> new HouseAccumulator()
            );
            accumulator.add(plugData.getAvg());
        }

        return houseAggregates;
    }

    private void persistPlugData(List<PlugData> plugDataList) throws SQLException {
        for (PlugData plugData : plugDataList) {
            plugInsertStatement.setInt(1, plugData.getHouseId());
            plugInsertStatement.setInt(2, plugData.getHouseholdId());
            plugInsertStatement.setInt(3, plugData.getPlugId());
            plugInsertStatement.setString(4, plugData.getYear());
            plugInsertStatement.setString(5, plugData.getMonth());
            plugInsertStatement.setString(6, plugData.getDay());
            plugInsertStatement.setInt(7, plugData.getSliceGap());
            plugInsertStatement.setInt(8, plugData.getSliceIndex());
            plugInsertStatement.setDouble(9, plugData.getValue());
            plugInsertStatement.setDouble(10, plugData.getCount());
            plugInsertStatement.setDouble(11, plugData.getAvg());
            plugInsertStatement.addBatch();
        }
        plugInsertStatement.executeBatch();
        plugInsertStatement.clearBatch();
    }

    private void persistHouseData(Map<HouseSliceKey, HouseAccumulator> houseAggregates) throws SQLException {
        for (Map.Entry<HouseSliceKey, HouseAccumulator> entry : houseAggregates.entrySet()) {
            HouseSliceKey key = entry.getKey();
            houseInsertStatement.setInt(1, key.houseId);
            houseInsertStatement.setString(2, key.year);
            houseInsertStatement.setString(3, key.month);
            houseInsertStatement.setString(4, key.day);
            houseInsertStatement.setInt(5, key.sliceGap);
            houseInsertStatement.setInt(6, key.sliceIndex);
            houseInsertStatement.setDouble(7, entry.getValue().average());
            houseInsertStatement.addBatch();
        }
        houseInsertStatement.executeBatch();
        houseInsertStatement.clearBatch();
    }

    private void updatePlugAnomalyStatistics(PlugData plugData, long triggerTimestampMillis) {
        double currentAverage = plugData.getAvg();
        if (currentAverage == 0.0d) {
            return;
        }

        PlugKey key = new PlugKey(plugData.getHouseId(), plugData.getHouseholdId(), plugData.getPlugId());
        RollingStatistic statistic = plugStatistics.computeIfAbsent(key, ignored -> new RollingStatistic());
        if (statistic.isEmpty()) {
            statistic.initialize(currentAverage);
            return;
        }

        checkAnomalies(
            "PLUG",
            plugData.getHouseId(),
            plugData.getHouseholdId(),
            plugData.getPlugId(),
            currentAverage,
            statistic,
            triggerTimestampMillis
        );
        statistic.update(currentAverage);
    }

    private void updateHouseAnomalyStatistics(HouseSliceKey key, double currentAverage, long triggerTimestampMillis) {
        if (currentAverage == 0.0d) {
            return;
        }

        HouseKey houseKey = new HouseKey(key.houseId);
        RollingStatistic statistic = houseStatistics.computeIfAbsent(houseKey, ignored -> new RollingStatistic());
        if (statistic.isEmpty()) {
            statistic.initialize(currentAverage);
            return;
        }

        checkAnomalies(
            "HOUSE",
            key.houseId,
            null,
            null,
            currentAverage,
            statistic,
            triggerTimestampMillis
        );
        statistic.update(currentAverage);
    }

    private void checkAnomalies(
        String entityType,
        int houseId,
        Integer householdId,
        Integer plugId,
        double currentAverage,
        RollingStatistic statistic,
        long triggerTimestampMillis
    ) {
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
            windowSizeMinutes,
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

    private void initializeDatabase() {
        String plugInsertSql = "INSERT INTO plug_data "
            + "(house_id, household_id, plug_id, year, month, day, slice_gap, slice_index, value, count, avg) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            + "ON CONFLICT (house_id, household_id, plug_id, year, month, day, slice_gap, slice_index) "
            + "DO UPDATE SET value = EXCLUDED.value, count = EXCLUDED.count, avg = EXCLUDED.avg";
        String houseInsertSql = "INSERT INTO house_data "
            + "(house_id, year, month, day, slice_gap, slice_index, avg) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?) "
            + "ON CONFLICT (house_id, year, month, day, slice_gap, slice_index) "
            + "DO UPDATE SET avg = EXCLUDED.avg";

        try {
            connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
            connection.setAutoCommit(false);
            plugInsertStatement = connection.prepareStatement(plugInsertSql);
            houseInsertStatement = connection.prepareStatement(houseInsertSql);
            LOGGER.info("Connected to PostgreSQL at {}", jdbcUrl);
        } catch (SQLException exception) {
            throw new IllegalStateException("Unable to initialize PostgreSQL connection", exception);
        }
    }

    private void closeDatabase() {
        try {
            if (plugInsertStatement != null) {
                plugInsertStatement.close();
            }
            if (houseInsertStatement != null) {
                houseInsertStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
            LOGGER.info("Closed PostgreSQL resources successfully");
        } catch (SQLException exception) {
            LOGGER.warn("Failed to close PostgreSQL resources cleanly", exception);
        }
    }

    private void rollbackDatabase(Exception exception) {
        if (connection == null) {
            return;
        }

        try {
            connection.rollback();
            LOGGER.warn("Rolled back PostgreSQL transaction after processing failure", exception);
        } catch (SQLException rollbackException) {
            LOGGER.error("Failed to roll back PostgreSQL transaction", rollbackException);
        }
    }

    private String getDataStreamId() {
        return DATA_STREAM_PREFIX + windowSizeMinutes + "m";
    }

    private String getPunctuationStreamId() {
        return PUNCTUATION_STREAM_PREFIX + windowSizeMinutes + "m";
    }

    private static String getSetting(String name, String defaultValue) {
        String systemValue = System.getProperty(name);
        if (systemValue != null && !systemValue.trim().isEmpty()) {
            return systemValue;
        }
        String environmentValue = System.getenv(name);
        return environmentValue == null || environmentValue.trim().isEmpty() ? defaultValue : environmentValue;
    }

    private static int getIntegerSetting(String name, int defaultValue) {
        try {
            return Integer.parseInt(getSetting(name, String.valueOf(defaultValue)));
        } catch (NumberFormatException exception) {
            return defaultValue;
        }
    }

    private static final class PlugKey {

        private final int houseId;
        private final int householdId;
        private final int plugId;

        private PlugKey(int houseId, int householdId, int plugId) {
            this.houseId = houseId;
            this.householdId = householdId;
            this.plugId = plugId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof PlugKey)) {
                return false;
            }
            PlugKey plugKey = (PlugKey) other;
            return houseId == plugKey.houseId
                && householdId == plugKey.householdId
                && plugId == plugKey.plugId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(houseId, householdId, plugId);
        }
    }

    private static final class HouseKey {

        private final int houseId;

        private HouseKey(int houseId) {
            this.houseId = houseId;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof HouseKey)) {
                return false;
            }
            return houseId == ((HouseKey) other).houseId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(houseId);
        }
    }

    private static final class HouseSliceKey {

        private final int houseId;
        private final String year;
        private final String month;
        private final String day;
        private final int sliceIndex;
        private final int sliceGap;

        private HouseSliceKey(int houseId, String year, String month, String day, int sliceIndex, int sliceGap) {
            this.houseId = houseId;
            this.year = year;
            this.month = month;
            this.day = day;
            this.sliceIndex = sliceIndex;
            this.sliceGap = sliceGap;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof HouseSliceKey)) {
                return false;
            }
            HouseSliceKey key = (HouseSliceKey) other;
            return houseId == key.houseId
                && sliceIndex == key.sliceIndex
                && sliceGap == key.sliceGap
                && Objects.equals(year, key.year)
                && Objects.equals(month, key.month)
                && Objects.equals(day, key.day);
        }

        @Override
        public int hashCode() {
            return Objects.hash(houseId, year, month, day, sliceIndex, sliceGap);
        }
    }

    private static final class HouseAccumulator {

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

        private boolean isEmpty() {
            return count == 0L;
        }

        private void initialize(double value) {
            min = value;
            max = value;
            average = value;
            count = 1L;
        }

        private void update(double value) {
            long updatedCount = count + 1L;
            average = (average * count + value) / updatedCount;
            count = updatedCount;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
    }
}
