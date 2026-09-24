package com.storm.iotdata.functions;

import com.storm.iotdata.models.PlugData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * PostgreSQL persistence helper for the average bolt.
 *
 * The helper stores plug aggregates and the corresponding house aggregates
 * using the keys defined by the PostgreSQL schema in init.sql.
 */
public class DB_store {

	private static final Logger LOGGER = LoggerFactory.getLogger(DB_store.class);

	private static final String DEFAULT_JDBC_URL = "jdbc:postgresql://postgresql:5432/iotdata";
	private static final String DEFAULT_JDBC_USER = "postgres";
	private static final String DEFAULT_JDBC_PASSWORD = "postgres";

	private final String jdbcUrl;
	private final String jdbcUser;
	private final String jdbcPassword;
	private transient Connection connection;

	/**
	 * Creates a PostgreSQL store using environment variables when provided.
	 */
	public DB_store() {
		this.jdbcUrl = getSetting("JDBC_URL", DEFAULT_JDBC_URL);
		this.jdbcUser = getSetting("JDBC_USER", DEFAULT_JDBC_USER);
		this.jdbcPassword = getSetting("JDBC_PASSWORD", DEFAULT_JDBC_PASSWORD);
	}

	/**
	 * Opens the PostgreSQL connection used by this store.
	 */
	public void initialize() {
		try {
			connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
			connection.setAutoCommit(false);
			LOGGER.info("Connected to PostgreSQL at {}", jdbcUrl);
		} catch (SQLException exception) {
			throw new IllegalStateException("Unable to initialize PostgreSQL connection", exception);
		}
	}

	/**
	 * Persists plug aggregates and house aggregates in one transaction.
	 *
	 * @param plugDataList Completed plug aggregates.
	 * @return true when all records were committed.
	 */
	public boolean pushPlugData(Stack<PlugData> plugDataList) {
		if (plugDataList.isEmpty()) {
			return true;
		}

		String plugSql = "INSERT INTO plug_data "
			+ "(house_id, household_id, plug_id, year, month, day, slice_gap, slice_index, value, count, avg) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
			+ "ON CONFLICT (house_id, household_id, plug_id, year, month, day, slice_gap, slice_index) "
			+ "DO UPDATE SET value = EXCLUDED.value, count = EXCLUDED.count, avg = EXCLUDED.avg";
		String houseSql = "INSERT INTO house_data "
			+ "(house_id, year, month, day, slice_gap, slice_index, avg) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?) "
			+ "ON CONFLICT (house_id, year, month, day, slice_gap, slice_index) "
			+ "DO UPDATE SET avg = EXCLUDED.avg";

		Map<HouseSliceKey, HouseAccumulator> houseData = new HashMap<HouseSliceKey, HouseAccumulator>();

		try (PreparedStatement plugStatement = connection.prepareStatement(plugSql);
			PreparedStatement houseStatement = connection.prepareStatement(houseSql)) {
			for (PlugData plugData : plugDataList) {
				plugStatement.setInt(1, plugData.getHouseId());
				plugStatement.setInt(2, plugData.getHouseholdId());
				plugStatement.setInt(3, plugData.getPlugId());
				plugStatement.setString(4, plugData.getYear());
				plugStatement.setString(5, plugData.getMonth());
				plugStatement.setString(6, plugData.getDay());
				plugStatement.setInt(7, plugData.getSliceGap());
				plugStatement.setInt(8, plugData.getSliceIndex());
				plugStatement.setDouble(9, plugData.getValue());
				plugStatement.setDouble(10, plugData.getCount());
				plugStatement.setDouble(11, plugData.getAvg());
				plugStatement.addBatch();

				HouseSliceKey key = new HouseSliceKey(plugData);
				houseData.computeIfAbsent(key, ignored -> new HouseAccumulator()).add(plugData.getAvg());
			}
			plugStatement.executeBatch();

			for (Map.Entry<HouseSliceKey, HouseAccumulator> entry : houseData.entrySet()) {
				HouseSliceKey key = entry.getKey();
				houseStatement.setInt(1, key.houseId);
				houseStatement.setString(2, key.year);
				houseStatement.setString(3, key.month);
				houseStatement.setString(4, key.day);
				houseStatement.setInt(5, key.sliceGap);
				houseStatement.setInt(6, key.sliceIndex);
				houseStatement.setDouble(7, entry.getValue().average());
				houseStatement.addBatch();
			}
			houseStatement.executeBatch();
			connection.commit();
			return true;
		} catch (SQLException exception) {
			rollback(exception);
			return false;
		}
	}

	/**
	 * Closes the PostgreSQL connection.
	 */
	public void close() {
		if (connection == null) {
			return;
		}

		try {
			connection.close();
			LOGGER.info("Closed PostgreSQL resources successfully");
		} catch (SQLException exception) {
			LOGGER.warn("Failed to close PostgreSQL resources cleanly", exception);
		}
	}

	private void rollback(SQLException exception) {
		try {
			connection.rollback();
			LOGGER.error("Rolled back PostgreSQL transaction", exception);
		} catch (SQLException rollbackException) {
			LOGGER.error("Failed to roll back PostgreSQL transaction", rollbackException);
		}
	}

	private static String getSetting(String name, String defaultValue) {
		String systemValue = System.getProperty(name);
		if (systemValue != null && !systemValue.trim().isEmpty()) {
			return systemValue;
		}
		String environmentValue = System.getenv(name);
		return environmentValue == null || environmentValue.trim().isEmpty() ? defaultValue : environmentValue;
	}

	private static final class HouseSliceKey {

		private final int houseId;
		private final String year;
		private final String month;
		private final String day;
		private final int sliceGap;
		private final int sliceIndex;

		private HouseSliceKey(PlugData plugData) {
			this.houseId = plugData.getHouseId();
			this.year = plugData.getYear();
			this.month = plugData.getMonth();
			this.day = plugData.getDay();
			this.sliceGap = plugData.getSliceGap();
			this.sliceIndex = plugData.getSliceIndex();
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
			return houseId == key.houseId && sliceGap == key.sliceGap && sliceIndex == key.sliceIndex
				&& year.equals(key.year) && month.equals(key.month) && day.equals(key.day);
		}

		@Override
		public int hashCode() {
			int result = houseId;
			result = 31 * result + year.hashCode();
			result = 31 * result + month.hashCode();
			result = 31 * result + day.hashCode();
			result = 31 * result + sliceGap;
			return 31 * result + sliceIndex;
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
}