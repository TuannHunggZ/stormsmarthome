package com.storm.iotdata.storm;

import java.io.PrintStream;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.net.URL;
import java.net.HttpURLConnection;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import com.storm.iotdata.functions.DB_store;
import com.storm.iotdata.functions.MQTT_publisher;
import com.storm.iotdata.models.*;

import com.google.gson.JsonObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

public class Bolt_forecast extends BaseRichBolt {
    private StormConfig config;
    private int gap;
    private HashMap<String,HouseData> houseDataList;
    private HashMap<String,HouseholdData> householdDataList;
    private HashMap<String,DeviceData> deviceDataList;
    private OutputCollector _collector;

    public Bolt_forecast(Integer gap, StormConfig config){
        this.gap = gap;
        this.config = config;
        houseDataList = new HashMap<String,HouseData>();
        householdDataList = new HashMap<String,HouseholdData>();
        deviceDataList = new HashMap<String,DeviceData>();
    }


    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try{
            if(input.getSourceStreamId().equals("trigger")) {
                sendForecastToBackend("http://mysql:8080/forecast");
                _collector.ack(input);
            } else if(input.getSourceStreamId().equals("data")) {
                if(input.getValueByField("type").equals(HouseData.class)){
                    HouseData data = (HouseData) input.getValueByField("data");
                    houseDataList.put(data.getUniqueId(), data);
                    _collector.ack(input);
                }
                else if(input.getValueByField("type").equals(HouseholdData.class)){
                    HouseholdData data = (HouseholdData) input.getValueByField("data");
                    householdDataList.put(data.getUniqueId(), data);
                    _collector.ack(input);
                }
                else if(input.getValueByField("type").equals(DeviceData.class)){
                    DeviceData data = (DeviceData) input.getValueByField("data");
                    deviceDataList.put(data.getUniqueId(), data);
                    _collector.ack(input);
                }
                else {
                    _collector.fail(input);
                }
            } else {
                _collector.fail(input);
            }
        } catch (Exception ex){
            ex.printStackTrace();
            _collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }

    public void sendForecastToBackend(String backendUrl) {
        try {
            // Create connection
            URL url = new URL(backendUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            Gson gson = new com.google.gson.GsonBuilder().setPrettyPrinting().create();
            JsonObject payload = new JsonObject();
            payload.addProperty("gap", gap);
            payload.add("houseData", gson.toJsonTree(houseDataList));
            payload.add("householdData", gson.toJsonTree(householdDataList));
            payload.add("deviceData", gson.toJsonTree(deviceDataList));
	        payload.addProperty("notificationBrokerURL", config.getNotificationBrokerURL());
	        payload.addProperty("mqttTopicPrefix", config.getMqttTopicPrefix());

            // Write payload
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = payload.toString().getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            // Read response
            if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(conn.getInputStream(), "utf-8"))) {
                    StringBuilder response = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }
                    
                    // Parse JSON response
                    JsonObject jsonResponse = new Gson().fromJson(response.toString(), JsonObject.class);
                    
                    // Update local data lists
                    if (jsonResponse.has("remainingHouseData")) {
                        Type houseType = new TypeToken<HashMap<String, HouseData>>(){}.getType();
                        HashMap<String, HouseData> updatedHouseData = new Gson().fromJson(
                            jsonResponse.get("remainingHouseData"), houseType);
                        houseDataList.clear();
                        houseDataList.putAll(updatedHouseData);
                    }
                    
                    if (jsonResponse.has("remainingHouseholdData")) {
                        Type householdType = new TypeToken<HashMap<String, HouseholdData>>(){}.getType();
                        HashMap<String, HouseholdData> updatedHouseholdData = new Gson().fromJson(
                            jsonResponse.get("remainingHouseholdData"), householdType);
                        householdDataList.clear();
                        householdDataList.putAll(updatedHouseholdData);
                    }
                    
                    if (jsonResponse.has("remainingDeviceData")) {
                        Type deviceType = new TypeToken<HashMap<String, DeviceData>>(){}.getType();
                        HashMap<String, DeviceData> updatedDeviceData = new Gson().fromJson(
                            jsonResponse.get("remainingDeviceData"), deviceType);
                        deviceDataList.clear();
                        deviceDataList.putAll(updatedDeviceData);
                    }
                }
            } else {
                System.out.println("Backend request failed with code: " + conn.getResponseCode());
            }

            PrintStream fileOut = new PrintStream("/apache-storm/output.txt");
	        Gson gson2 = new com.google.gson.GsonBuilder().setPrettyPrinting().create();
            fileOut.println("========= HouseData (JSON Format) =========");
            fileOut.println(gson2.toJson(houseDataList));
            
            fileOut.println("\n========= HouseholdData (JSON Format) =========");
            fileOut.println(gson2.toJson(householdDataList));
            
            fileOut.println("\n========= DeviceData (JSON Format) =========");
            fileOut.println(gson2.toJson(deviceDataList));


            conn.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
