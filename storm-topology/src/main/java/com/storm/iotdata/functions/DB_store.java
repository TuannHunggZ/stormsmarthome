package com.storm.iotdata.functions;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storm.iotdata.models.PlugData;
import com.storm.iotdata.models.StormConfig;

/**
 * PostgreSQL persistence helper that follows the asynchronous locker pattern
 * used by the legacy topology.
 */
public class DB_store {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DB_store.class);

	/**
     * Opens a PostgreSQL connection using the topology configuration.
     *
     * @return A connection with auto-commit disabled.
	 */
    public static Connection initConnection() throws SQLException {
		try {
            String jdbcUrl = StormConfig.getJdbcUrl();
            Connection connection = DriverManager.getConnection(
                jdbcUrl,
                StormConfig.getJdbcUser(),
                StormConfig.getJdbcPassword()
            );
            connection.setAutoCommit(false);
            LOGGER.info("Connected to PostgreSQL at {}", jdbcUrl);
            return connection;
		} catch (SQLException exception) {
			throw new IllegalStateException("Unable to initialize PostgreSQL connection", exception);
		}
	}

    /**
     * Starts asynchronous persistence unless another worker owns the locker.
     *
     * @param dataList Completed plug aggregates.
     * @param locker Per-window persistence lock file.
     * @return true when a persistence worker was started.
     */
	public static boolean pushPlugData(Stack<PlugData> dataList, File locker) {
		try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            }
            new PlugData2DB(dataList, locker).start();
            return true;
        } catch (Exception exception) {
            LOGGER.error("Failed to start plug data persistence worker", exception);
            return false;
        }
	}
}

/**
 * Writes one completed plug-data batch and retries after a temporary failure.
 */
class PlugData2DB extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlugData2DB.class);

    private Stack<PlugData> dataList;
    private File locker;

    public PlugData2DB(Stack<PlugData> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();

            Connection conn = DB_store.initConnection();
            try (PreparedStatement tempSql = conn.prepareStatement(
                "INSERT INTO plug_data " +
                "(house_id, household_id, plug_id, year, month, day, slice_gap, slice_index, value, count, avg) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT (house_id, household_id, plug_id, year, month, day, slice_gap, slice_index) " +
                "DO UPDATE SET " +
                "value = EXCLUDED.value, " +
                "count = EXCLUDED.count, " +
                "avg = EXCLUDED.avg"
            )) {
                for (PlugData data : dataList) {
                    tempSql.setInt(1, data.getHouseId());
                    tempSql.setInt(2, data.getHouseholdId());
                    tempSql.setInt(3, data.getPlugId());
                    tempSql.setString(4, data.getYear());
                    tempSql.setString(5, data.getMonth());
                    tempSql.setString(6, data.getDay());
                    tempSql.setInt(7, data.getSliceGap());
                    tempSql.setInt(8, data.getSliceIndex());
                    tempSql.setDouble(9, data.getValue());
                    tempSql.setDouble(10, data.getCount());
                    tempSql.setDouble(11, data.getAvg());
                    tempSql.addBatch();
                }
                tempSql.executeBatch();
            }
            conn.commit();
            conn.close();
            locker.delete();
            dataList = null;
        } catch (Exception exception) {
            LOGGER.error("Failed to persist plug data; retrying", exception);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                LOGGER.warn("Plug data persistence retry was interrupted", interruptedException);
            }

            locker.delete();
            new PlugData2DB(dataList, locker).start();
        }
    }
}