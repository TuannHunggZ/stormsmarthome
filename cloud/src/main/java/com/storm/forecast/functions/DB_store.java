package com.storm.forecast.functions;

import java.io.File;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.storm.forecast.models.*;

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
