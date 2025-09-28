/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata.storm;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import com.storm.iotdata.models.*;
import com.storm.iotdata.functions.*;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 *
 * @author hiiamlala
 */
public class Bolt_avg extends BaseRichBolt {
    public Long processSpeed = Long.valueOf(0);
    public Integer deviceDataUpdateFailCount = 0;
    public Integer deviceNotiUpdateFailCount = 0;
    public Integer devicePropUpdateFailCount = 0;
    private StormConfig config;
    public Integer gap;
    public Integer triggerCount = 0;
    public Integer cleanTrigger = 5; //older than 5*gap will be clean 
    public Double total = Double.valueOf(0);
    public HashMap<String, DeviceData> deviceDataList = new HashMap<String, DeviceData>();
    public HashMap<String, DeviceProp> devicePropList = new HashMap<String, DeviceProp>();

    public Bolt_avg(Integer gap, StormConfig config) {
        this.gap = gap;
        this.config = config;
        devicePropList = DB_store.initDevicePropList();
    }
    
    private OutputCollector _collector;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        _collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            if(tuple.getSourceStreamId().equals("trigger")){
                if(((++triggerCount)%gap)==0){
                    Integer triggerInterval = (Integer) tuple.getValueByField("trigger");
                    SpoutProp spoutProp = (SpoutProp) tuple.getValueByField("spoutProp");

                    Long startExec = System.currentTimeMillis();
                    Stack<String> needClean = new Stack<String>();
                    Stack<DeviceData> needSave = new Stack<DeviceData>();
                    Stack<DeviceNotification> deviceNotificationList = new Stack<DeviceNotification>();

                    //Init data for save and clean procedure
                    for(String key : deviceDataList.keySet()){
                        DeviceData data = deviceDataList.get(key);
                        if(!data.isSaved()){
                            _collector.emit("data", new Values(data.getClass(), data));
                            needSave.push(data);
                        }
                        else if(data.isSaved() && (System.currentTimeMillis()-data.getLastUpdate())>(cleanTrigger*gap*60000)){
                            needClean.push(key);
                        }
                    }

                    //DB store data
                    if(DB_store.pushDeviceData(needSave, new File("./tmp/deviceData2db-" + gap + ".lck"))){
                        deviceDataUpdateFailCount=0;
                        for(DeviceData deviceData : needSave){
                            deviceDataList.get(deviceData.getUniqueId()).save();
                        }
                    }
                    else if(++deviceDataUpdateFailCount >= 3) {
                        new File("./tmp/deviceData2db-" + gap + ".lck").delete();
                    }

                    //DB store prop
                    Stack<DeviceProp> tempDevicePropList = new Stack<DeviceProp>();
                    tempDevicePropList.addAll(devicePropList.values());
                    if(DB_store.pushDeviceProp(tempDevicePropList, new File("./tmp/deviceProp2db-"+ gap + ".lck"))){
                        devicePropUpdateFailCount=0;
                        for(DeviceProp deviceProp : tempDevicePropList){
                            devicePropList.get(deviceProp.getDeviceUniqueId()).save();
                        }
                    }
                    else if(++devicePropUpdateFailCount >= 3) {
                        new File("./tmp/deviceProp2db-"+ gap + ".lck").delete();
                    }

                    //Check outlier
                    for(DeviceData deviceData : needSave){
                        String devicePropUniqueId = deviceData.getDeviceUniqueId();
                        DeviceProp deviceProp = devicePropList.getOrDefault(devicePropUniqueId, new DeviceProp(deviceData.houseId, deviceData.householdId, deviceData.deviceId, this.gap));
                        if(config.isDeviceCheckMax() && deviceProp.getMax()!=0 && (deviceData.getAvg()-deviceProp.getMax())>=(deviceProp.getMax()*config.getDeviceLogGap()/100)){
                            //Check over Max
                            deviceNotificationList.push(new DeviceNotification(1, deviceData, deviceProp));
                        }
                        if(config.isDeviceCheckAvg() && deviceProp.getAvg()!=0 && (deviceData.getAvg()-deviceProp.getAvg())>=(deviceProp.getAvg()*config.getDeviceLogGap()/100)){
                            //Check over Avg
                            deviceNotificationList.push(new DeviceNotification(0, deviceData, deviceProp));
                        }
                        if(config.isDeviceCheckMin() && deviceProp.getMin()!=0 && (deviceProp.getMin()-deviceData.getAvg())<=(deviceProp.getMin()*config.getDeviceLogGap()/100)){
                            //Check under Min
                            deviceNotificationList.push(new DeviceNotification(-1, deviceData, deviceProp));
                        }
                        //Save data_prop
                        devicePropList.put(devicePropUniqueId, deviceProp.addValue(deviceData.getAvg()));
                    }
                    
                    //Save Noti
                    if(DB_store.pushDeviceNotification(deviceNotificationList, new File("./tmp/devicenoti2db-" + gap + ".lck"))){
                        deviceNotiUpdateFailCount = 0;
                        //Noti saved
                        //Publish Noti
                        if(config.isNotificationMQTT()){
                            MQTT_publisher.deviceNotificationsPublish(deviceNotificationList, config.getSpoutBrokerURL(), config.getMqttTopicPrefix(), new File("./tmp/devicenoti2mqtt-"+ gap +".lck"));
                        }
                    }
                    else if(++deviceNotiUpdateFailCount>=3){
                        new File("./tmp/devicenoti2db-" + gap + ".lck").delete();
                    }
                    
                    //Logging
                    Long execTime = System.currentTimeMillis() - startExec;

                    Stack<String> logs = new Stack<String>();
                    logs.push(String.format("[Bolt_avg_%-3d] Process speed: %-10d mess/s\n", gap, processSpeed/triggerInterval/triggerCount));
                    logs.push(String.format("[Bolt_avg_%-3d] Noti list: %-10d\n", gap, deviceNotificationList.size()));
                    logs.push(String.format("[Bolt_avg_%-3d] Total: %-10d | Already saved: %-10d | Need save: %-10d | Need clean: %-10d\n",gap, deviceDataList.size(), deviceDataList.size()-needSave.size(), needSave.size(), needClean.size()));
                    logs.push(String.format("[Bolt_avg_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000));

                    MQTT_publisher.stormLogPublish(logs, config.getNotificationBrokerURL(), config.getMqttTopicPrefix(), new File("./tmp/bolt-avg-"+ gap +"-log-publish.lck"));
                    for(String data : logs){
                        System.out.println(data);
                    }
                    try {
                        FileWriter log = new FileWriter(new File("./tmp/bolt_avg_"+ gap +".tmp"), false);
                        PrintWriter pwOb = new PrintWriter(log , false);
                        pwOb.flush();
                        for(String data : logs){
                            log.write(data);
                        }
                        pwOb.close();
                        log.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    
                    //Clean garbage
                    for(String key : needClean){
                        deviceDataList.remove(key);
                    }

                    _collector.emit("trigger", new Values(triggerInterval, spoutProp));
                    processSpeed = Long.valueOf(0);
                    triggerCount = 0;
                }
                _collector.ack(tuple);
            }
            else if (tuple.getSourceStreamId().equals("data")) {
                Integer houseId         = (Integer) tuple.getValueByField("houseId");
                Integer householdId     = (Integer)tuple.getValueByField("householdId");
                Integer deviceId        = (Integer)tuple.getValueByField("deviceId");
                String year             = (String)tuple.getValueByField("year");
                String month            = (String)tuple.getValueByField("month");
                String day              = (String)tuple.getValueByField("day");
                Integer index           = (Integer) tuple.getValueByField("sliceIndex");
                Double  value           = (Double) tuple.getValueByField("value");
                DeviceData deviceData   = new DeviceData(houseId, householdId, deviceId, year, month, day, index, gap);
                deviceDataList.put(deviceData.getUniqueId(), deviceDataList.getOrDefault(deviceData.getUniqueId(), deviceData ).increaseValue(value));
                processSpeed++;
                _collector.ack(tuple);
            }
            else{
                _collector.fail(tuple);
            }
        }catch (Exception ex){
            ex.printStackTrace();
            _collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("data", new Fields("type","data"));
        declarer.declareStream("trigger", new Fields("trigger", "spoutProp"));
    }
    
}