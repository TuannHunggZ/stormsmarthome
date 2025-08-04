package com.storm.forecast;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Stack;
import java.sql.*;
import com.storm.forecast.models.*;
import com.storm.forecast.functions.*;


@RestController
@RequestMapping("/forecast")
public class ForecastController {

    @PostMapping
    public ResponseEntity<?> receiveForecastData(@RequestBody ForecastRequest request) {
        if (request.getHouseData() == null || request.getHouseholdData() == null || request.getDeviceData() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Missing required data fields"));
        }

        int gap = request.getGap();
        String brokerURL = request.getNotificationBrokerURL();
        String topicPrefix = request.getMqttTopicPrefix();
        HashMap<String,HouseData> houseDataList = request.getHouseData();
        HashMap<String,HouseholdData> householdDataList = request.getHouseholdData();
        HashMap<String,DeviceData> deviceDataList = request.getDeviceData();
        Stack<String> logs = new Stack<String>();

        Long start = System.currentTimeMillis();
        HashMap<String, HouseData> houseDataForecast = forecast(houseDataList);
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
        
        start = System.currentTimeMillis();
        HashMap<String, DeviceData> deviceDataForecast = forecast((HashMap<String, DeviceData>) request.getDeviceData());
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

        MQTT_publisher.stormLogPublish(logs, brokerURL, topicPrefix, new File("./tmp/bolt-forecast-"+ gap +"-log-publish.lck"));
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
        
        Map<String, Object> response = new HashMap<>();
        response.put("message", "Data received and processed successfully");
        response.put("remainingHouseData", houseDataList);
        response.put("remainingHouseholdData", householdDataList);
        response.put("remainingDeviceData", deviceDataList);
        return ResponseEntity.ok(response);
    }

    public static <E> HashMap<String, E> forecast(HashMap<String,E> inputData){
        HashMap<String, E> result = new HashMap<String, E>();
        try{
            if(inputData.size()!=0){
                try (Connection conn = DB_store.initConnection()){
                    if(inputData.values().toArray()[0] instanceof HouseData){
                        for(String key : inputData.keySet()){
                            HouseData ele = (HouseData) inputData.get(key);
                            Double median = getMedian(DB_store.queryBefore(ele, conn));
                            Double forecastValue = ele.getAvg();
                            if(median > 0){
                                forecastValue = (forecastValue + median)/2;
                            }
                            result.put(key, (E) new HouseData(ele.getHouseId(), ele.getTimeslice().getNextTimeslice(2), forecastValue));
                        }
                    }
                    else if(inputData.values().toArray()[0] instanceof HouseholdData){
                        for(String key : inputData.keySet()){
                            HouseholdData ele = (HouseholdData) inputData.get(key);
                            Double median = getMedian(DB_store.queryBefore(ele, conn));
                            Double forecastValue = ele.getAvg();
                            if(median > 0){
                                forecastValue = (forecastValue + median)/2;
                            }
                            result.put(key, (E) new HouseholdData(ele.getHouseId(), ele.getHouseholdId() , ele.getTimeslice().getNextTimeslice(2), forecastValue));
                        }
                    }
                    else if(inputData.values().toArray()[0] instanceof DeviceData){
                        for(String key : inputData.keySet()){
                            DeviceData ele = (DeviceData) inputData.get(key);
                            Double median = getMedian(DB_store.queryBefore(ele, conn));
                            Double forecastValue = ele.getAvg();
                            if(median > 0){
                                forecastValue = (forecastValue + median)/2;
                            }
                            result.put(key, (E) new DeviceData(ele.getHouseId(), ele.getHouseholdId(), ele.getDeviceId(), ele.getTimeslice().getNextTimeslice(2), forecastValue));
                        }
                    }
                    conn.close();
                }
            }
        } catch (Exception ex){
            ex.printStackTrace();
        } finally {
            return result;
        }
    }

    public static <E> Double getMedian(HashMap<String, E> beforeData){
        Double median = Double.valueOf(0);
        if(beforeData.size()>0){
            ArrayList<E> beforeAvgs = new ArrayList<>(beforeData.values());
            beforeAvgs.sort(new Comparator<E>(){
                @Override
                public int compare(E data1, E data2) {
                    if(data1 instanceof HouseData){
                        HouseData temp1 = (HouseData) data1;
                        HouseData temp2 = (HouseData) data2;
                        return Double.compare(temp1.getAvg(), temp2.getAvg());
                    }
                    else if(data1 instanceof HouseholdData){
                        HouseholdData temp1 = (HouseholdData) data1;
                        HouseholdData temp2 = (HouseholdData) data2;
                        return Double.compare(temp1.getAvg(), temp2.getAvg());
                    }
                    else if(data1 instanceof DeviceData){
                        DeviceData temp1 = (DeviceData) data1;
                        DeviceData temp2 = (DeviceData) data2;
                        return Double.compare(temp1.getAvg(), temp2.getAvg());
                    }
                    return 0;
                }
            });
            if(beforeAvgs.size()%2==0){
                if(beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)) instanceof HouseData){
                    HouseData temp1 = (HouseData) beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2));
                    HouseData temp2 = (HouseData) beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)-1);
                    median = (temp1.getAvg() + temp2.getAvg())/2;
                }
                else if(beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)) instanceof HouseholdData){
                    HouseholdData temp1 = (HouseholdData) beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2));
                    HouseholdData temp2 = (HouseholdData) beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)-1);
                    median = (temp1.getAvg() + temp2.getAvg())/2;
                }
                else if(beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)) instanceof HouseholdData){
                    DeviceData temp1 = (DeviceData) beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2));
                    DeviceData temp2 = (DeviceData) beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)-1);
                    median = (temp1.getAvg() + temp2.getAvg())/2;
                }
            }
            else {
                if(beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)) instanceof HouseData){
                    HouseData temp = (HouseData) beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2));
                    median = temp.getAvg();
                }
                else if(beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)) instanceof HouseholdData){
                    HouseholdData temp = (HouseholdData) beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2));
                    median = temp.getAvg();
                }
                else if(beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2)) instanceof DeviceData){
                    DeviceData temp = (DeviceData) beforeAvgs.get(Math.floorDiv(beforeAvgs.size(), 2));
                    median = temp.getAvg();
                }
            }
        }
        return median;
    }
}