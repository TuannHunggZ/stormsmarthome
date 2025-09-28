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

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import com.storm.iotdata.functions.DB_store;
import com.storm.iotdata.functions.MQTT_publisher;
import com.storm.iotdata.models.*;
import com.google.gson.JsonObject;

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
            if(input.getSourceStreamId().equals("trigger")){
                try {
                    com.google.gson.Gson gson = new com.google.gson.GsonBuilder().setPrettyPrinting().create();
                    
                    PrintStream fileOut = new PrintStream("/apache-storm/output.txt");
                    
                    // fileOut.println("========= HouseData (JSON Format) =========");
                    // fileOut.println(gson.toJson(houseDataList));
                    
                    // fileOut.println("\n========= HouseholdData (JSON Format) =========");
                    // fileOut.println(gson.toJson(householdDataList));
                    
                    // fileOut.println("\n========= DeviceData (JSON Format) =========");
                    // fileOut.println(gson.toJson(deviceDataList));

		    String currentDirectory = new File("").getAbsolutePath();
		    fileOut.println("Thư mục hiện tại: " + currentDirectory);
		    fileOut.println("Config: " + config.getNotificationBrokerURL() + " " + config.getMqttTopicPrefix());

                    String houseJson = gson.toJson(houseDataList);
                    String householdJson = gson.toJson(householdDataList);
                    String deviceJson = gson.toJson(deviceDataList);

                    JsonObject payload = new JsonObject();
		    payload.addProperty("gap", gap);
                    payload.add("houseData", gson.toJsonTree(houseDataList));
                    payload.add("householdData", gson.toJsonTree(householdDataList));
                    payload.add("deviceData", gson.toJsonTree(deviceDataList));
		    payload.addProperty("notificationBrokerURL", config.getNotificationBrokerURL());
		    payload.addProperty("mqttTopicPrefix", config.getMqttTopicPrefix());

                    sendForecastToBackend("http://mysql:8080/forecast", payload);

                    // fileOut.println(payload);
                    
                    fileOut.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }

                Stack<String> logs = new Stack<String>();
                //Start forecast
                Long start = System.currentTimeMillis();
                HashMap<String, HouseData> houseDataForecast= forecast(houseDataList);
                Stack<HouseData> tempHouseDataForecast = new Stack<HouseData>();
                tempHouseDataForecast.addAll(houseDataForecast.values());
                if(DB_store.pushHouseDataForecast("v0", tempHouseDataForecast, new File("./tmp/houseDataForecast2db-" + gap + ".lck"))){
                    for(String key : houseDataForecast.keySet()){
                        houseDataList.remove(key);
                    }
                    //Log HouseData
                    logs.add(String.format("[Bolt_forecast_%d] HouseData forecast took %.2fs\n", gap, (float)(System.currentTimeMillis()-start)/1000));
                    logs.add(String.format("[Bolt_forecast_%d] HouseData Total: %-10d | Saved and clean: %-10d\n", gap, houseDataList.size(), tempHouseDataForecast.size()));
                    //Cleanning
                    houseDataForecast = null;
                    tempHouseDataForecast = null;
                }
                else {
                    logs.add(String.format("[Bolt_forecast_%d] HouseData forecast not saved\n", gap));
                }

                start = System.currentTimeMillis();
                HashMap<String, HouseholdData> householdDataForecast = forecast(householdDataList);
                Stack<HouseholdData> tempHouseholdDataForecast = new Stack<HouseholdData>();
                tempHouseholdDataForecast.addAll(householdDataForecast.values());
                if(DB_store.pushHouseholdDataForecast("v0", tempHouseholdDataForecast, new File("./tmp/householdDataForecast2db-" + gap + ".lck"))){
                    for(String key : householdDataForecast.keySet()){
                        householdDataList.remove(key);
                    }
                    //Log HouseholdData
                    logs.add(String.format("[Bolt_forecast_%d] HouseholdData forecast took %.2fs\n", gap, (float)(System.currentTimeMillis()-start)/1000));
                    logs.add(String.format("[Bolt_forecast_%d] HouseholdData Total: %-10d | Saved and clean: %-10d\n", gap, householdDataList.size(), tempHouseholdDataForecast.size()));
                    //Cleaning
                    householdDataForecast = null;
                    tempHouseholdDataForecast = null;
                }
                else {
                    logs.add(String.format("[Bolt_forecast_%d] HouseholdData forecast not saved\n", gap));
                }

                HashMap<String, DeviceData> deviceDataForecast = forecast(deviceDataList);
                Stack<DeviceData> tempDeviceDataForecast = new Stack<DeviceData>();
                tempDeviceDataForecast.addAll(deviceDataForecast.values());
                if(DB_store.pushDeviceDataForecast("v0", tempDeviceDataForecast, new File("./tmp/deviceDataForecast2db-" + gap + ".lck"))){
                    for(String key : deviceDataForecast.keySet()){
                        deviceDataList.remove(key);
                    }
                    //Log HouseData
                    logs.add(String.format("[Bolt_forecast_%d] DeviceData forecast took %.2fs\n", gap, (float)(System.currentTimeMillis()-start)/1000));
                    logs.add(String.format("[Bolt_forecast_%d] DeviceData Total: %-10d | Saved and clean: %-10d\n", gap, deviceDataList.size(), tempDeviceDataForecast.size()));
                    //Cleaning
                    deviceDataForecast = null;
                    tempDeviceDataForecast = null;
                }
                else {
                    logs.add(String.format("[Bolt_forecast_%d] DeviceData forecast not saved\n", gap));
                }

                MQTT_publisher.stormLogPublish(logs, config.getNotificationBrokerURL(), config.getMqttTopicPrefix(), new File("./tmp/bolt-forecast-"+ gap +"-log-publish.lck"));
                for(String data : logs){
                    System.out.println(data);
                }
                try {
                    FileWriter log = new FileWriter(new File("./tmp/bolt_forecast_"+ gap +".tmp"), false);
                    PrintWriter pwOb = new PrintWriter(log , false);
                    pwOb.flush();
                    for(String data : logs){
                        log.write(data);
                    }
                    pwOb.close();
                    log.close();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                _collector.ack(input);
            }
            else if(input.getSourceStreamId().equals("data")){
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
            }
            else {
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

    public static void sendForecastToBackend(String backendUrl, JsonObject jsonData) {
        try {
            URL url = new URL(backendUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonData.toString().getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int code = conn.getResponseCode();
            System.out.println("Backend response code: " + code);

            conn.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}