package com.storm.iotdata.functions;

import java.io.File;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.storm.iotdata.models.*;

import org.yaml.snakeyaml.Yaml;

public class DB_store {

    private Connection conn;

    private static InputStream getConfig() {
        return DB_store.class.getClassLoader().getResourceAsStream("config/cred.yaml");
    }

    public static Connection initConnection() throws ClassNotFoundException, SQLException, FileNotFoundException {
        Yaml yaml = new Yaml();
        InputStream inputStream = getConfig();
        Map<String, Object> obj = yaml.load(inputStream);
        String dbURL = "jdbc:mysql://" + obj.get("db_url") + "/" + obj.get("db_name");
        String userName = (String) obj.get("db_user");
        String password = (String) obj.get("db_pass");
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection(dbURL, userName, password);
        conn.setAutoCommit(true);
        return conn;
    }

    public static Connection initConnection(Boolean autoCommit) throws ClassNotFoundException, SQLException, FileNotFoundException {
        Yaml yaml = new Yaml();
        InputStream inputStream = getConfig();
        Map<String, Object> obj = yaml.load(inputStream);
        String dbURL = "jdbc:mysql://" + obj.get("db_url") + "/" + obj.get("db_name");
        String userName = (String) obj.get("db_user");
        String password = (String) obj.get("db_pass");
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection(dbURL, userName, password);
        conn.setAutoCommit(autoCommit);
        return conn;
    }

    public DB_store() {
        try {
            this.conn = DB_store.initConnection();
        } catch (SQLException sql) {
            System.out.println("SQLException: " + sql.getMessage());
            System.out.println("SQLState: " + sql.getSQLState());
            System.out.println("Erro: " + sql.getErrorCode());
            System.out.println("StackTrace: " + sql.getStackTrace());
        } catch (Exception ex) {
            System.out.println("connect failure!");
            ex.printStackTrace();
        }
    }

    public static boolean purgeData() {
        try {
            Connection conn = DB_store.initConnection();
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("drop database iot_data");
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        } finally {
            if (DB_store.initData())
                return true;
            else
                return false;
        }
    }

    public static boolean initData() {
        try {
            Yaml yaml = new Yaml();
            InputStream inputStream = getConfig();
            Map<String, Object> obj = yaml.load(inputStream);
            String dbName = (String) obj.get("db_name");
            String dbURL = "jdbc:mysql://" + obj.get("db_url");
            String userName = (String) obj.get("db_user");
            String password = (String) obj.get("db_pass");
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(dbURL, userName, password);
            Statement stmt = conn.createStatement();
            try {
                stmt.executeUpdate(String.format("create database if not exists %s", dbName));
                stmt.execute(String.format("use %s", dbName));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table if not exists device_data (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, device_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, value DOUBLE UNSIGNED NOT NULL, count DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, household_id, device_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table if not exists household_data(house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, household_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table if not exists house_data(house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table if not exists forecast_meta_data(version VARCHAR(4) NOT NULL, slice_gap INT UNSIGNED NOT NULL, count DOUBLE UNSIGNED DEFAULT 0, mean DOUBLE UNSIGNED DEFAULT 0, variance DOUBLE UNSIGNED DEFAULT 0, standart_deviation DOUBLE UNSIGNED DEFAULT 0, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(slice_gap, version))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table if not exists device_prop (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, device_id INT UNSIGNED NOT NULL, slice_gap INT UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, count DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, household_id, device_id, slice_gap))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table if not exists device_notification (type INT SIGNED NOT NULL, house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, device_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, value DOUBLE UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(type, house_id, household_id, device_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table if not exists household_prop (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, slice_gap INT UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, count DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, household_id, slice_gap))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table if not exists household_notification (type INT SIGNED NOT NULL, house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, value DOUBLE UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(type, house_id, household_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table if not exists house_prop (house_id INT UNSIGNED NOT NULL, slice_gap INT UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, count DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, slice_gap))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            try {
                stmt.executeUpdate(
                        "create table if not exists house_notification (type INT SIGNED NOT NULL, house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL, slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, value DOUBLE UNSIGNED NOT NULL, min DOUBLE UNSIGNED NOT NULL, max DOUBLE UNSIGNED NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(type, house_id, year, month, day, slice_gap, slice_index))");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            initForecastTable("v0");
            stmt.close();
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static boolean initForecastTable(String table) {
        try {
            Connection conn = DB_store.initConnection();
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("create table if not exists house_data_forecast_" + table
                    + " (house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, slice_gap, slice_index))");
            stmt.executeUpdate("create table if not exists household_data_forecast_" + table
                    + " (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, household_id, year, month, day, slice_gap, slice_index))");
            stmt.executeUpdate("create table if not exists device_data_forecast_" + table
                    + " (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, device_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, household_id, device_id, year, month, day, slice_gap, slice_index))");
            stmt.close();
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static boolean purgeForecastTable(String table) {
        try {
            Connection conn = DB_store.initConnection();
            Statement stmt = conn.createStatement();
            stmt.execute("drop table if exists house_data_forecast_" + table);
            stmt.execute("drop table if exists household_data_forecast_" + table);
            stmt.execute("drop table if exists device_data_forecast_" + table);
            stmt.executeUpdate("create table if not exists house_data_forecast_" + table
                    + " (house_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, slice_gap, slice_index))");
            stmt.executeUpdate("create table if not exists household_data_forecast_" + table
                    + " (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, slice_gap, slice_index))");
            stmt.executeUpdate("create table if not exists device_data_forecast_" + table
                    + " (house_id INT UNSIGNED NOT NULL, household_id INT UNSIGNED NOT NULL, device_id INT UNSIGNED NOT NULL, year VARCHAR(4) NOT NULL, month VARCHAR(2) NOT NULL, day VARCHAR(2) NOT NULL,slice_gap INT UNSIGNED NOT NULL, slice_index INT NOT NULL, avg DOUBLE UNSIGNED NOT NULL, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(house_id, year, month, day, slice_gap, slice_index))");
            stmt.close();
            conn.close();
            return true;
        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    public static boolean pushHouseData(Stack<HouseData> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseData2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushHouseHoldData(Stack<HouseholdData> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseholdData2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushDeviceData(Stack<DeviceData> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new DeviceData2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushHouseDataForecast(String version, Stack<HouseData> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseDataForecast2DB(version, dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushHouseholdDataForecast(String version, Stack<HouseholdData> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseholdDataForecast2DB(version, dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushDeviceDataForecast(String version, Stack<DeviceData> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new DeviceDataForecast2DB(version, dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushDeviceNotification(Stack<DeviceNotification> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new DeviceNotification2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushHouseholdNotification(Stack<HouseholdNotification> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseholdNotification2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean pushHouseNotification(Stack<HouseNotification> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseNotification2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public Stack<HouseData> queryHouseData(int house_id, String year, String month, String day, int slice_gap, int slice_index) {
        Stack<HouseData> result = new Stack<HouseData>();
        String sql = "SELECT * FROM house_data WHERE ";
        try {
            Statement stmt = this.conn.createStatement();
            Boolean condition = false;
            if (house_id != -1) {
                sql += "house_id=" + house_id + " AND ";
                condition = true;
            }
            if (year.length() != 0) {
                sql += "year=\"" + year + "\" AND ";
                condition = true;
            }
            if (month.length() != 0) {
                sql += "month=\"" + month + "\" AND ";
                condition = true;
            }
            if (day.length() != 0) {
                sql += "day=\"" + day + "\" AND ";
                condition = true;
            }
            if (house_id != -1) {
                sql += "slice_gap=" + slice_gap + " AND ";
                condition = true;
            }
            if (slice_index != -1) {
                sql += "slice_index=" + slice_index + " AND ";
                condition = true;
            }
            try (ResultSet rs = stmt.executeQuery(sql.substring(0, sql.length() - (condition ? 5 : 7)))) {
                while (rs.next()) {
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_index"), rs.getInt("slice_gap"),
                            rs.getDouble("avg")));
                }
                return result;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            this.reConnect();
            System.out.println("[ERROR] " + sql);
            return queryHouseData(house_id, year, month, day, slice_gap, slice_index);
        }
    }

    public Stack<HouseData> queryBeforeV0(int house_id, String year, String month, String day, int slice_gap,
            int slice_index) {
        Stack<HouseData> result = new Stack<HouseData>();
        String sql = "SELECT * FROM house_data WHERE house_id=" + house_id + " AND year=\"" + year + "\" AND month=\""
                + month + "\" AND day=\"" + day + "\" AND slice_gap=" + slice_gap;
        try {
            Statement stmt = this.conn.createStatement();
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && slice_gap < 0
                    && slice_index < 0) {
                return new Stack<>();
            }
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .after(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        break;
                    } else if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .equals(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        if (rs.getInt("slice_index") > slice_index) {
                            break;
                        }
                    }
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_index"), rs.getInt("slice_gap"),
                            rs.getDouble("avg")));
                }
                return result;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[ERROR] " + sql);
            this.reConnect();
            return queryBeforeV0(house_id, year, month, day, slice_gap, slice_index);
        }
    }

    public Stack<HouseData> queryBeforeV1(int house_id, String year, String month, String day, int slice_gap,
            int slice_index) {
        String sql = "SELECT * FROM house_data WHERE house_id=" + house_id + " AND slice_gap=" + slice_gap;
        Stack<HouseData> result = new Stack<HouseData>();
        try {
            Statement stmt = this.conn.createStatement();
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && slice_gap < 0
                    && slice_index < 0) {
                return new Stack<>();
            }
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .after(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        break;
                    } else if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .equals(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        if (rs.getInt("slice_index") > slice_index) {
                            break;
                        }
                    }
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_index"), rs.getInt("slice_gap"),
                            rs.getDouble("avg")));
                }
                return result;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[ERROR] " + sql);
            this.reConnect();
            return queryBeforeV1(house_id, year, month, day, slice_gap, slice_index);
        }
    }

    public Stack<HouseData> queryBeforeV2(int house_id, String year, String month, String day, int slice_gap,
            int slice_index) {
        Stack<HouseData> result = new Stack<HouseData>();
        String sql = "";
        try {
            Boolean end = false;
            Statement stmt = this.conn.createStatement();
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && slice_gap < 0
                    && slice_index < 0) {
                return new Stack<>();
            }
            Calendar query_cal = Calendar.getInstance();
            query_cal.setTime(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1, Integer.valueOf(day)));
            do {
                sql = "SELECT * FROM house_data WHERE house_id=" + house_id + " AND year=\""
                        + query_cal.get(Calendar.YEAR) + "\" AND month=\""
                        + String.format("%02d", (query_cal.get(Calendar.MONTH) + 1)) + "\" AND day=\""
                        + String.format("%02d", query_cal.get(Calendar.DAY_OF_MONTH)) + "\" AND slice_gap=" + slice_gap
                        + " AND slice_index=" + slice_index;
                query_cal.add(Calendar.WEEK_OF_YEAR, -1);
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    if (rs.next()) {
                        result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                                rs.getString("day"), rs.getInt("slice_index"), rs.getInt("slice_gap"),
                                rs.getDouble("avg")));
                    } else {
                        end = true;
                    }
                }
            } while (!end);
            return result;
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[ERROR] " + sql);
            this.reConnect();
            return queryBeforeV2(house_id, year, month, day, slice_gap, slice_index);
        }
    }

    public Stack<HouseData> queryBeforeV3(int house_id, String year, String month, String day, int slice_gap,
            int slice_index) {
        Stack<HouseData> result = new Stack<HouseData>();
        String sql = "SELECT * FROM house_data WHERE house_id=" + house_id + " AND slice_index=" + slice_index
                + " AND slice_gap=" + slice_gap;
        try {
            Statement stmt = this.conn.createStatement();
            if (house_id < 0 && year.length() == 0 && month.length() == 0 && day.length() == 0 && slice_gap < 0
                    && slice_index < 0) {
                return new Stack<>();
            }
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .after(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        break;
                    } else if (new Date(Integer.valueOf(rs.getString("year")) - 1900,
                            Integer.valueOf(rs.getString("month")) - 1, Integer.valueOf(rs.getString("day")))
                                    .equals(new Date(Integer.valueOf(year) - 1900, Integer.valueOf(month) - 1,
                                            Integer.valueOf(day)))) {
                        if (rs.getInt("slice_index") > slice_index) {
                            break;
                        }
                    }
                    result.push(new HouseData(rs.getInt("house_id"), rs.getString("year"), rs.getString("month"),
                            rs.getString("day"), rs.getInt("slice_index"), rs.getInt("slice_gap"),
                            rs.getDouble("avg")));
                }
                return result;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("[ERROR] " + sql);
            this.reConnect();
            return queryBeforeV3(house_id, year, month, day, slice_gap, slice_index);
        }
    }

    public void reConnect() {
        try {
            this.conn = DB_store.initConnection();
        } catch (Exception ex) {
            System.out.println("connect failure! Retrying...");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            this.reConnect();
            ex.printStackTrace();
        }
    }

    public void close() {
        try {
            this.conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static HashMap<String, DeviceProp> initDevicePropList() {
        Connection conn;
        HashMap<String, DeviceProp> result = new HashMap<String, DeviceProp>();
        String sql = "SELECT * FROM device_prop";
        try {
            conn = DB_store.initConnection();
            Statement stmt = conn.createStatement();
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    DeviceProp tempDeviceProp = new DeviceProp(rs.getInt("house_id"), rs.getInt("household_id"),
                            rs.getInt("device_id"), rs.getInt("slice_gap"), rs.getDouble("min"), rs.getDouble("avg"),
                            rs.getDouble("max"), rs.getDouble("count"), true);
                    result.put(tempDeviceProp.getDeviceUniqueId(), tempDeviceProp);
                }
                rs.close();
                stmt.close();
                conn.close();
                return result;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return new HashMap<String, DeviceProp>();
        }
    }

    public static boolean pushDeviceProp(Stack<DeviceProp> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new DeviceProp2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static HashMap<String, HouseholdProp> initHouseholdPropList() {
        Connection conn;
        HashMap<String, HouseholdProp> result = new HashMap<String, HouseholdProp>();
        String sql = "SELECT * FROM household_prop";
        try {
            conn = DB_store.initConnection();
            Statement stmt = conn.createStatement();
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    HouseholdProp tempHouseholdProp = new HouseholdProp(rs.getInt("house_id"),
                            rs.getInt("household_id"), rs.getInt("slice_gap"), rs.getDouble("min"), rs.getDouble("avg"),
                            rs.getDouble("max"), rs.getDouble("count"), true);
                    result.put(tempHouseholdProp.getHouseholdUniqueId(), tempHouseholdProp);
                }
                rs.close();
                stmt.close();
                conn.close();
                return result;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return new HashMap<String, HouseholdProp>();
        }
    }

    public static boolean pushHouseholdProp(Stack<HouseholdProp> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseholdProp2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static HashMap<String, HouseProp> initHousePropList() {
        Connection conn;
        HashMap<String, HouseProp> result = new HashMap<String, HouseProp>();
        String sql = "SELECT * FROM house_prop";
        try {
            conn = DB_store.initConnection();
            Statement stmt = conn.createStatement();
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    HouseProp tempHouseProp = new HouseProp(rs.getInt("house_id"), rs.getInt("slice_gap"),
                            rs.getDouble("min"), rs.getDouble("avg"), rs.getDouble("max"), rs.getDouble("count"), true);
                    result.put(tempHouseProp.getHouseUniqueId(), tempHouseProp);
                }
                rs.close();
                stmt.close();
                conn.close();
                return result;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return new HashMap<String, HouseProp>();
        }
    }

    public static boolean pushHouseProp(Stack<HouseProp> dataList, File locker) {
        try {
            if (locker.exists() || dataList.isEmpty()) {
                return false;
            } else {
                new HouseProp2DB(dataList, locker).start();
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static HashMap<String, HouseData> queryBefore(HouseData houseData, Connection conn){
        HashMap<String, HouseData> result = new HashMap<String, HouseData>();
        try{
            try (PreparedStatement tempSql = conn.prepareStatement("select * from house_data where house_id=? and slice_gap=? and slice_index=?")){
                tempSql.setInt(1, houseData.getHouseId());
                tempSql.setInt(2, houseData.getGap());
                tempSql.setInt(3, houseData.getTimeslice().getNextTimeslice(2).getIndex());
                try (ResultSet rs = tempSql.executeQuery()) {
                    while (rs.next()) {
                        Integer rsHouseId = rs.getInt("house_id");
                        String rsYear = rs.getString("year");
                        String rsMonth = rs.getString("month");
                        String rsDay = rs.getString("day");
                        Integer rsIndex = rs.getInt("slice_index");
                        Integer rsGap = rs.getInt("slice_gap");
                        Double rsAvg = rs.getDouble("avg");
                        
                        if(Integer.parseInt(rsYear) <= Integer.parseInt(houseData.getYear())){
                            if(Integer.parseInt(rsMonth) <= Integer.parseInt(houseData.getMonth())){
                                if(Integer.parseInt(rsDay) <= Integer.parseInt(houseData.getDay())){
                                    HouseData rsHouseData = new HouseData(rsHouseId, rsYear, rsMonth, rsDay, rsIndex, rsGap, rsAvg);
                                    result.put(rsHouseData.getUniqueId(), rsHouseData);
                                }
                            }
                        }
                    }
                    rs.close();
                    tempSql.close();
                }
            }
        } catch(Exception ex){
            ex.printStackTrace();
        }
        return result;
    }

    public static HashMap<String, HouseholdData> queryBefore(HouseholdData householdData, Connection conn){
        HashMap<String, HouseholdData> result = new HashMap<String, HouseholdData>();
        try{
            try (PreparedStatement tempSql = conn.prepareStatement("select * from household_data where house_id=? and household_id=? and slice_gap=? and slice_index=?")){
                tempSql.setInt(1, householdData.getHouseId());
                tempSql.setInt(2, householdData.getHouseholdId());
                tempSql.setInt(3, householdData.getGap());
                tempSql.setInt(4, householdData.getTimeslice().getNextTimeslice(2).getIndex());
                try (ResultSet rs = tempSql.executeQuery()) {
                    while (rs.next()) {
                        Integer rsHouseId = rs.getInt("house_id");
                        Integer rsHouseholdId = rs.getInt("household_id");
                        String rsYear = rs.getString("year");
                        String rsMonth = rs.getString("month");
                        String rsDay = rs.getString("day");
                        Integer rsIndex = rs.getInt("slice_index");
                        Integer rsGap = rs.getInt("slice_gap");
                        Double rsAvg = rs.getDouble("avg");
                        
                        if(Integer.parseInt(rsYear) < Integer.parseInt(householdData.getYear())){
                            HouseholdData rsHouseholdData = new HouseholdData(rsHouseId, rsHouseholdId, rsYear, rsMonth, rsDay, rsIndex, rsGap, rsAvg);
                            result.put(rsHouseholdData.getUniqueId(), rsHouseholdData);
                        }
                        else if(Integer.parseInt(rsYear) == Integer.parseInt(householdData.getYear())){
                            if(Integer.parseInt(rsMonth) < Integer.parseInt(householdData.getMonth())){
                                HouseholdData rsHouseholdData = new HouseholdData(rsHouseId, rsHouseholdId, rsYear, rsMonth, rsDay, rsIndex, rsGap, rsAvg);
                                result.put(rsHouseholdData.getUniqueId(), rsHouseholdData);
                            }
                            else if(Integer.parseInt(rsMonth) == Integer.parseInt(householdData.getMonth())){
                                if(Integer.parseInt(rsDay) < Integer.parseInt(householdData.getDay())){
                                    HouseholdData rsHouseholdData = new HouseholdData(rsHouseId, rsHouseholdId, rsYear, rsMonth, rsDay, rsIndex, rsGap, rsAvg);
                                    result.put(rsHouseholdData.getUniqueId(), rsHouseholdData);
                                }
                            }
                        }
                    }
                    rs.close();
                    tempSql.close();
                }
            }
        } catch(Exception ex){
            ex.printStackTrace();
        }
        return result;
    }

    public static HashMap<String, DeviceData> queryBefore(DeviceData deviceData, Connection conn){
        HashMap<String, DeviceData> result = new HashMap<String, DeviceData>();
        try{
            try (PreparedStatement tempSql = conn.prepareStatement("select * from device_data where house_id=? and household_id=? and device_id=? and slice_gap=? and slice_index=?")){
                tempSql.setInt(1, deviceData.getHouseId());
                tempSql.setInt(2, deviceData.getHouseholdId());
                tempSql.setInt(3, deviceData.getDeviceId());
                tempSql.setInt(4, deviceData.getGap());
                tempSql.setInt(5, deviceData.getTimeslice().getNextTimeslice(2).getIndex());
                try (ResultSet rs = tempSql.executeQuery()) {
                    while (rs.next()) {
                        Integer rsHouseId = rs.getInt("house_id");
                        Integer rsHouseholdId = rs.getInt("household_id");
                        Integer rsDeviceId = rs.getInt("device_id");
                        String rsYear = rs.getString("year");
                        String rsMonth = rs.getString("month");
                        String rsDay = rs.getString("day");
                        Integer rsIndex = rs.getInt("slice_index");
                        Integer rsGap = rs.getInt("slice_gap");
                        Double rsAvg = rs.getDouble("avg");
                        
                        if(Integer.parseInt(rsYear) <= Integer.parseInt(deviceData.getYear())){
                            if(Integer.parseInt(rsMonth) <= Integer.parseInt(deviceData.getMonth())){
                                if(Integer.parseInt(rsDay) <= Integer.parseInt(deviceData.getDay())){
                                    DeviceData rsDeviceData = new DeviceData(rsHouseId, rsHouseholdId, rsDeviceId, rsYear, rsMonth, rsDay, rsIndex, rsGap, rsAvg);
                                    result.put(rsDeviceData.getUniqueId(), rsDeviceData);
                                }
                            }
                        }
                    }
                    rs.close();
                    tempSql.close();
                }
            }
        } catch(Exception ex){
            ex.printStackTrace();
        }
        return result;
    }
}

class DeviceProp2DB extends Thread {
    private Stack<DeviceProp> dataList;
    private File locker;

    public DeviceProp2DB(Stack<DeviceProp> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                        "insert into device_prop (house_id,household_id,device_id,slice_gap,min,avg,max,count) values (?,?,?,?,?,?,?,?) on duplicate key update min=VALUES(min), avg=VALUES(avg), max=VALUES(max), count=VALUES(count)");
            for (DeviceProp data : dataList) {
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getHouseholdId());
                tempSql.setInt(3, data.getDeviceId());
                tempSql.setInt(4, data.getSliceGap());
                tempSql.setDouble(5, data.getMin());
                tempSql.setDouble(6, data.getAvg());
                tempSql.setDouble(7, data.getMax());
                tempSql.setDouble(8, data.getCount());
                tempSql.addBatch();
            }
            tempSql.executeBatch();
            conn.commit();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
            locker.delete();
            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new DeviceProp2DB(dataList, locker).start();
            ex.printStackTrace();
        }
    }
}

class HouseholdProp2DB extends Thread {
    private Stack<HouseholdProp> dataList;
    private File locker;

    public HouseholdProp2DB(Stack<HouseholdProp> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                        "insert into household_prop (house_id,household_id,slice_gap,min,avg,max,count) values (?,?,?,?,?,?,?) on duplicate key update min=VALUES(min), avg=VALUES(avg), max=VALUES(max), count=VALUES(count)");
            for (HouseholdProp data : dataList) {
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getHouseholdId());
                tempSql.setInt(3, data.getSliceGap());
                tempSql.setDouble(4, data.getMin());
                tempSql.setDouble(5, data.getAvg());
                tempSql.setDouble(6, data.getMax());
                tempSql.setDouble(7, data.getCount());
                tempSql.addBatch();
            }
            tempSql.executeBatch();
            conn.commit();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
            locker.delete();
            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new HouseholdProp2DB(dataList, locker).start();
            ex.printStackTrace();
        }
    }
}

class HouseProp2DB extends Thread {
    private Stack<HouseProp> dataList;
    private File locker;

    public HouseProp2DB(Stack<HouseProp> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                        "insert into house_prop (house_id,slice_gap,min,avg,max,count) values (?,?,?,?,?,?) on duplicate key update min=VALUES(min), avg=VALUES(avg), max=VALUES(max), count=VALUES(count)");
            for (HouseProp data : dataList) {
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getSliceGap());
                tempSql.setDouble(3, data.getMin());
                tempSql.setDouble(4, data.getAvg());
                tempSql.setDouble(5, data.getMax());
                tempSql.setDouble(6, data.getCount());
                tempSql.addBatch();
            }
            tempSql.executeBatch();
            conn.commit();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
            locker.delete();
            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new HouseProp2DB(dataList, locker).start();
            ex.printStackTrace();
        }
    }
}

class DeviceData2DB extends Thread {
    private Stack<DeviceData> dataList;
    private File locker;

    public DeviceData2DB(Stack<DeviceData> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                        "insert into device_data (house_id,household_id,device_id,year,month,day,slice_gap,slice_index,value,count,avg) values (?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=VALUES(value), count=VALUES(count), avg=VALUES(avg)");
            for (DeviceData data : dataList) {
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getHouseholdId());
                tempSql.setInt(3, data.getDeviceId());
                tempSql.setString(4, data.getYear());
                tempSql.setString(5, data.getMonth());
                tempSql.setString(6, data.getDay());
                tempSql.setInt(7, data.getGap());
                tempSql.setInt(8, data.getIndex());
                tempSql.setDouble(9, data.getValue());
                tempSql.setDouble(10, data.getCount());
                tempSql.setDouble(11, data.getAvg());
                tempSql.addBatch();
            }
            tempSql.executeBatch();
            conn.commit();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
            locker.delete();
            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new DeviceData2DB(dataList, locker).start();
            ex.printStackTrace();
        }
    }
}

class HouseholdData2DB extends Thread {
    private Stack<HouseholdData> dataList;
    private File locker;

    public HouseholdData2DB(Stack<HouseholdData> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                        "insert into household_data (house_id,household_id,year,month,day,slice_gap,slice_index,avg) values (?,?,?,?,?,?,?,?) on duplicate key update avg=VALUES(avg)");
            for (HouseholdData data : dataList) {
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getHouseholdId());
                tempSql.setString(3, data.getYear());
                tempSql.setString(4, data.getMonth());
                tempSql.setString(5, data.getDay());
                tempSql.setInt(6, data.getGap());
                tempSql.setInt(7, data.getIndex());
                tempSql.setDouble(8, data.getValue());
                tempSql.addBatch();
            }
            tempSql.executeBatch();
            conn.commit();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
            locker.delete();
            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new HouseholdData2DB(dataList, locker).start();
            ex.printStackTrace();
        }
    }
}

class HouseData2DB extends Thread {
    private Stack<HouseData> dataList;
    private File locker;

    public HouseData2DB(Stack<HouseData> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                        "insert into house_data (house_id,year,month,day,slice_gap,slice_index,avg) values (?,?,?,?,?,?,?) on duplicate key update avg=VALUES(avg)");
            for (HouseData data : dataList) {
                tempSql.setInt(1, data.getHouseId());
                tempSql.setString(2, data.getYear());
                tempSql.setString(3, data.getMonth());
                tempSql.setString(4, data.getDay());
                tempSql.setInt(5, data.getGap());
                tempSql.setInt(6, data.getIndex());
                tempSql.setDouble(7, data.getValue());
                tempSql.addBatch();
                // String statementText = tempSql.toString();
                // sql += statementText.substring(statementText.slice_indexOf(": ") + 2) + ",";
            }
            // sql = sql.substring(0, sql.length() - 1) + "";
            // stmt.executeUpdate(sql);
            tempSql.executeBatch();
            conn.commit();
            conn.close();
            locker.delete();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);

            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new HouseData2DB(dataList, locker).start();
            ex.printStackTrace();
        }
    }
}

class DeviceDataForecast2DB extends Thread {
    private Stack<DeviceData> dataList;
    private File locker;
    private String version;

    public DeviceDataForecast2DB(String version, Stack<DeviceData> dataList, File locker) {
        this.version = version;
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                        "insert into device_data_forecast_"+ version +" (house_id,household_id,device_id,year,month,day,slice_gap,slice_index,avg) values (?,?,?,?,?,?,?,?,?) on duplicate key update avg=VALUES(avg)");
            for (DeviceData data : dataList) {
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getHouseholdId());
                tempSql.setInt(3, data.getDeviceId());
                tempSql.setString(4, data.getYear());
                tempSql.setString(5, data.getMonth());
                tempSql.setString(6, data.getDay());
                tempSql.setInt(7, data.getGap());
                tempSql.setInt(8, data.getIndex());
                tempSql.setDouble(9, data.getAvg());
                tempSql.addBatch();
            }
            tempSql.executeBatch();
            conn.commit();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
            locker.delete();
            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new DeviceDataForecast2DB(version, dataList, locker).start();
            ex.printStackTrace();
        }
    }
}

class HouseholdDataForecast2DB extends Thread {
    private Stack<HouseholdData> dataList;
    private File locker;
    private String version;

    public HouseholdDataForecast2DB(String version, Stack<HouseholdData> dataList, File locker) {
        this.version = version;
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                        "insert into household_data_forecast_"+ version +" (house_id,household_id,year,month,day,slice_gap,slice_index,avg) values (?,?,?,?,?,?,?,?) on duplicate key update avg=VALUES(avg)");
            for (HouseholdData data : dataList) {
                tempSql.setInt(1, data.getHouseId());
                tempSql.setInt(2, data.getHouseholdId());
                tempSql.setString(3, data.getYear());
                tempSql.setString(4, data.getMonth());
                tempSql.setString(5, data.getDay());
                tempSql.setInt(6, data.getGap());
                tempSql.setInt(7, data.getIndex());
                tempSql.setDouble(8, data.getValue());
                tempSql.addBatch();
            }
            tempSql.executeBatch();
            conn.commit();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);
            conn.close();
            locker.delete();
            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new HouseholdDataForecast2DB(version, dataList, locker).start();
            ex.printStackTrace();
        }
    }
}

class HouseDataForecast2DB extends Thread {
    private Stack<HouseData> dataList;
    private File locker;
    private String version;

    public HouseDataForecast2DB(String version, Stack<HouseData> dataList, File locker) {
        this.version = version;
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                        "insert into house_data_forecast_"+ version +" (house_id,year,month,day,slice_gap,slice_index,avg) values (?,?,?,?,?,?,?) on duplicate key update avg=VALUES(avg)");
            for (HouseData data : dataList) {
                tempSql.setInt(1, data.getHouseId());
                tempSql.setString(2, data.getYear());
                tempSql.setString(3, data.getMonth());
                tempSql.setString(4, data.getDay());
                tempSql.setInt(5, data.getGap());
                tempSql.setInt(6, data.getIndex());
                tempSql.setDouble(7, data.getValue());
                tempSql.addBatch();
                // String statementText = tempSql.toString();
                // sql += statementText.substring(statementText.slice_indexOf(": ") + 2) + ",";
            }
            // sql = sql.substring(0, sql.length() - 1) + "";
            // stmt.executeUpdate(sql);
            tempSql.executeBatch();
            conn.commit();
            conn.close();
            locker.delete();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);

            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new HouseDataForecast2DB(version, dataList, locker).start();
            ex.printStackTrace();
        }
    }
}

class DeviceNotification2DB extends Thread {
    private Stack<DeviceNotification> dataList;
    private File locker;

    public DeviceNotification2DB(Stack<DeviceNotification> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                        "insert into device_notification (type,house_id,household_id,device_id,year,month,day,slice_gap,slice_index,value,min,max,avg) values (?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=VALUES(value), min=VALUES(min), max=VALUES(max), avg=VALUES(avg)");
            for (DeviceNotification data : dataList) {
                tempSql.setInt(1, data.getType());
                tempSql.setInt(2, data.getHouseId());
                tempSql.setInt(3, data.getHouseholdId());
                tempSql.setInt(4, data.getDeviceId());
                tempSql.setString(5, data.getYear());
                tempSql.setString(6, data.getMonth());
                tempSql.setString(7, data.getDay());
                tempSql.setInt(8, data.getGap());
                tempSql.setInt(9, data.getIndex());
                tempSql.setDouble(10, data.getValue());
                tempSql.setDouble(11, data.getMin());
                tempSql.setDouble(12, data.getMax());
                tempSql.setDouble(13, data.getAvg());
                tempSql.addBatch();
            }
            tempSql.executeBatch();
            conn.commit();
            conn.close();
            locker.delete();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);

            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new DeviceNotification2DB(dataList, locker).start();
            ex.printStackTrace();
        }
    }
}

class HouseholdNotification2DB extends Thread {
    private Stack<HouseholdNotification> dataList;
    private File locker;

    public HouseholdNotification2DB(Stack<HouseholdNotification> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                    "insert into household_notification (type,house_id,household_id,year,month,day,slice_gap,slice_index,value,min,max,avg) values (?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=VALUES(value), min=VALUES(min), max=VALUES(max), avg=VALUES(avg)",
                    Statement.RETURN_GENERATED_KEYS);
            for (HouseholdNotification data : dataList) {
                tempSql.setInt(1, data.getType());
                tempSql.setInt(2, data.getHouseId());
                tempSql.setInt(3, data.getHouseholdId());
                tempSql.setString(4, data.getYear());
                tempSql.setString(5, data.getMonth());
                tempSql.setString(6, data.getDay());
                tempSql.setInt(7, data.getGap());
                tempSql.setInt(8, data.getIndex());
                tempSql.setDouble(9, data.getValue());
                tempSql.setDouble(10, data.getMin());
                tempSql.setDouble(11, data.getMax());
                tempSql.setDouble(12, data.getAvg());
                tempSql.addBatch();
            }
            tempSql.executeBatch();
            conn.commit();
            conn.close();
            locker.delete();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);

            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new HouseholdNotification2DB(dataList, locker).start();
            ex.printStackTrace();
        }
    }
}

class HouseNotification2DB extends Thread {
    private Stack<HouseNotification> dataList;
    private File locker;

    public HouseNotification2DB(Stack<HouseNotification> dataList, File locker) {
        this.dataList = dataList;
        this.locker = locker;
    }

    @Override
    public void run() {
        try {
            locker.createNewFile();
            locker.deleteOnExit();
            // Init connection
            Connection conn = DB_store.initConnection(false);
            // Init SQL
            Long start = System.currentTimeMillis();
            PreparedStatement tempSql = conn.prepareStatement(
                    "insert into house_notification (type,house_id,year,month,day,slice_gap,slice_index,value,min,max,avg) values (?,?,?,?,?,?,?,?,?,?,?) on duplicate key update value=VALUES(value), min=VALUES(min), max=VALUES(max), avg=VALUES(avg)",
                    Statement.RETURN_GENERATED_KEYS);
            for (HouseNotification data : dataList) {
                tempSql.setInt(1, data.getType());
                tempSql.setInt(2, data.getHouseId());
                tempSql.setString(3, data.getYear());
                tempSql.setString(4, data.getMonth());
                tempSql.setString(5, data.getDay());
                tempSql.setInt(6, data.getGap());
                tempSql.setInt(7, data.getIndex());
                tempSql.setDouble(8, data.getValue());
                tempSql.setDouble(9, data.getMin());
                tempSql.setDouble(10, data.getMax());
                tempSql.setDouble(11, data.getAvg());
                tempSql.addBatch();
            }
            tempSql.executeBatch();
            conn.commit();
            conn.close();
            locker.delete();
            System.out.printf("\n[" + locker.getName() + "] DB tooks %.2f s ("+ dataList.size() +" queries)\n",
                    (float) (System.currentTimeMillis() - start) / 1000);

            dataList = null;
        } catch (Exception ex) {
            try {
                System.out.printf("\n[%s] Wait for 10s then try again", locker.getName());
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            locker.delete();
            new HouseNotification2DB(dataList, locker).start();
            ex.printStackTrace();
        }
    }
}