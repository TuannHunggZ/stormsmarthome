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
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.storm.iotdata.functions.*;
import com.storm.iotdata.models.*;

/**
 *
 * @author hiiamlala
 */
public class Bolt_sum extends BaseRichBolt {
    public Integer houseDataUpdateFailCount = 0;
    public Integer houseNotiUpdateFailCount = 0;
    public Integer housePropUpdateFailCount = 0;
    public Integer householdDataUpdateFailCount = 0;
    public Integer householdNotiUpdateFailCount = 0;
    public Integer householdPropUpdateFailCount = 0;
    private StormConfig config;
    public Integer gap;
    public Integer cleanTrigger = 5; //older than 5*gap will be clean 
    private OutputCollector _collector;
    public HashMap<String, HashMap<Integer, HashMap<Integer, HashMap<String, DeviceData> > > > allData = new HashMap<String, HashMap<Integer,HashMap<Integer,HashMap<String, DeviceData> > > >();
    public HashMap <Integer, HashMap<String, HouseData> > finalHouseDataList = new HashMap <Integer, HashMap<String, HouseData> >();
    public HashMap <String, HashMap<String, HouseholdData> > finalHouseholdDataList = new HashMap <String, HashMap<String, HouseholdData> >();
    public HashMap <String, HouseProp> housePropList = new HashMap<String, HouseProp>();
    public HashMap <String, HouseholdProp> householdPropList = new HashMap<String, HouseholdProp>();
    
    public Bolt_sum(int gap, StormConfig config) {
        this.gap = gap;
        this.config = config;
        this.housePropList = DB_store.initHousePropList();
        this.householdPropList = DB_store.initHouseholdPropList();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("data", new Fields("type","data"));
        declarer.declareStream("trigger", new Fields("trigger", "spoutProp"));
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        _collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            if(tuple.getSourceStreamId().equals("trigger")){
                Integer triggerInterval = (Integer) tuple.getValueByField("trigger");
                SpoutProp spoutProp = (SpoutProp) tuple.getValueByField("spoutProp");

                Long startExec = System.currentTimeMillis();
                Integer allDataSize = 0;
                Integer finalHouseDataSize = 0;
                Integer finalHouseholdDataSize = 0;
                Stack<HouseData> houseDataNeedSave = new Stack<HouseData>();
                Stack<HouseholdData> householdDataNeedSave = new Stack<HouseholdData>();
                Stack<HouseData> houseDataNeedClean = new Stack<HouseData>();
                Stack<HouseholdData> householdDataNeedClean = new Stack<HouseholdData>();
                Stack<String> timesliceNeedClean = new Stack<String>();
                Stack<HouseNotification> houseNotificationList = new Stack<HouseNotification>();
                Stack<HouseholdNotification> householdNotificationList = new Stack<HouseholdNotification>();

                // Cal sum
                for(String timeslice : allData.keySet()){
                    HashMap<Integer, HashMap<Integer, HashMap<String, DeviceData> > > sliceData = allData.get(timeslice);
                    for(Integer houseId : sliceData.keySet()) {
                        Double houseValue = Double.valueOf(0);
                        HashMap<Integer,HashMap<String, DeviceData> > houseData = sliceData.get(houseId);
                        for(Integer householdId : houseData.keySet()) {
                            Double householdValue = Double.valueOf(0);
                            HashMap<String, DeviceData> householdData = houseData.get(householdId);
                            for(String dataId : householdData.keySet()) {
                                allDataSize++;
                                DeviceData data = householdData.get(dataId);
                                houseValue+=data.getAvg();
                                householdValue+=data.getAvg();
                            }
                            HouseholdData calHouseholdData = new HouseholdData(houseId, householdId, timeslice, householdValue);
                            HashMap<String, HouseholdData> tempFinalHouseholdData = finalHouseholdDataList.getOrDefault(calHouseholdData.getHouseholdUniqueId(), new HashMap<String, HouseholdData>());
                            HouseholdData tempHouseholdData = tempFinalHouseholdData.getOrDefault(calHouseholdData.getSliceId(), calHouseholdData);
                            tempHouseholdData.setValue(householdValue);
                            tempFinalHouseholdData.put(calHouseholdData.getSliceId(), tempHouseholdData);
                            finalHouseholdDataList.put(calHouseholdData.getHouseholdUniqueId(), tempFinalHouseholdData);
                        }
                        HouseData calHouseData = new HouseData(houseId, timeslice, houseValue);
                        HashMap<String, HouseData> tempFinalHouseData = finalHouseDataList.getOrDefault(calHouseData.getHouseId(), new HashMap<String, HouseData>());
                        HouseData tempHouseData = tempFinalHouseData.getOrDefault(calHouseData.getSliceId(), calHouseData);
                        tempHouseData.setValue(houseValue);
                        tempFinalHouseData.put(calHouseData.getSliceId(), tempHouseData);
                        finalHouseDataList.put(calHouseData.getHouseId(), tempFinalHouseData);
                    }
                }

                //Init data
                for(Integer houseId : finalHouseDataList.keySet()) {
                    HashMap<String, HouseData> tempFinalHouseData = finalHouseDataList.get(houseId);
                    for(String timeslice : tempFinalHouseData.keySet()){
                        finalHouseDataSize++;
                        HouseData houseData = tempFinalHouseData.get(timeslice);
                        if(!houseData.isSaved()){
                            _collector.emit("data", new Values(houseData.getClass(), houseData));
                            houseDataNeedSave.push(houseData);
                        }
                        else if((System.currentTimeMillis()-houseData.getLastUpdate())>(cleanTrigger*gap*1000)){
                            houseDataNeedClean.push(houseData);
                        }
                    }
                }

                for(String uniqueHouseholdId : finalHouseholdDataList.keySet()) {
                    HashMap<String, HouseholdData> tempFinalHouseholdData = finalHouseholdDataList.get(uniqueHouseholdId);
                    for(String timeslice : tempFinalHouseholdData.keySet()){
                        finalHouseholdDataSize++;
                        HouseholdData householdData = tempFinalHouseholdData.get(timeslice);
                        if(!householdData.isSaved()){
                            _collector.emit("data", new Values(householdData.getClass(), householdData));
                            householdDataNeedSave.push(householdData);
                        }
                        else if((System.currentTimeMillis()-householdData.getLastUpdate())>(cleanTrigger*gap*1000)){
                            householdDataNeedClean.push(householdData);
                        }
                    }
                }

                for(HouseData houseData : houseDataNeedClean){
                    Timeslice timeslice = houseData.getTimeslice();
                    if(timesliceNeedClean.contains(timeslice.getSliceId())) break;
                    for(HouseholdData householdData : householdDataNeedClean){
                        if(timeslice.isSameTimeslice(householdData.getTimeslice())){
                            timesliceNeedClean.push(timeslice.getSliceId());
                            break;
                        }
                    }
                }

                // DB store data
                if(houseDataNeedSave.size()!=0){
                    if(DB_store.pushHouseData(houseDataNeedSave, new File("./tmp/houseData2db-" + gap + ".lck"))){
                        houseDataUpdateFailCount=0;
                        for(HouseData data : houseDataNeedSave){
                            finalHouseDataList.get(data.getHouseId()).get(data.getSliceId()).save();
                        }
                    }
                }
                else if(++houseDataUpdateFailCount>=3){
                    new File("./tmp/houseData2db-" + gap + ".lck").delete();
                }

                if(householdDataNeedSave.size()!=0){
                    if(DB_store.pushHouseHoldData(householdDataNeedSave, new File("./tmp/householdData2db-" + gap + ".lck"))){
                        householdDataUpdateFailCount=0;
                        for(HouseholdData data : householdDataNeedSave){
                            finalHouseholdDataList.get(data.getHouseholdUniqueId()).get(data.getSliceId()).save();
                        }
                    }
                }
                else if(++householdDataUpdateFailCount>=3){
                    new File("./tmp/householdData2db-" + gap + ".lck").delete();
                }

                // DB store prop
                Stack<HouseProp> tempHousePropList = new Stack<HouseProp>();
                tempHousePropList.addAll(housePropList.values());
                if(DB_store.pushHouseProp(tempHousePropList, new File("./tmp/houseProp2db-"+ gap +".lck"))){
                    housePropUpdateFailCount=0;
                    for(HouseProp houseProp : tempHousePropList){
                        housePropList.get(houseProp.getHouseUniqueId()).save();
                    }
                }
                else if(++housePropUpdateFailCount>=3){
                    new File("./tmp/houseProp2db-"+ gap +".lck").delete();
                }

                Stack<HouseholdProp> tempHouseholdPropList = new Stack<HouseholdProp>();
                tempHouseholdPropList.addAll(householdPropList.values());
                if(DB_store.pushHouseholdProp(tempHouseholdPropList, new File("./tmp/householdProp2db-"+ gap +".lck"))){
                    householdPropUpdateFailCount=0;
                    for(HouseholdProp householdProp : tempHouseholdPropList){
                        householdPropList.get(householdProp.getHouseholdUniqueId()).save();
                    }
                }
                else if(++householdPropUpdateFailCount>=3){
                    new File("./tmp/householdProp2db-"+ gap +".lck").delete();
                }

                // Check Outlier
                for(HouseData houseData : houseDataNeedSave){
                    HouseProp houseProp = housePropList.getOrDefault(houseData.getHouseUniqueId(), new HouseProp(houseData.getHouseId(), houseData.getGap()));
                    // Check min
                    if(config.isHouseCheckMin() && houseProp.getMin()!=0 && (houseProp.getMin() - houseData.getAvg()) <= (houseProp.getMin()*config.getHouseLogGap()/100)){
                        houseNotificationList.push(new HouseNotification(-1, houseData, houseProp));
                    }
                    // Check avg
                    if(config.isHouseCheckAvg() && houseProp.getAvg()!=0 && (houseData.getAvg() - houseProp.getAvg()) >= (houseProp.getAvg()*config.getHouseLogGap()/100)){
                        houseNotificationList.push(new HouseNotification(0, houseData, houseProp));
                    }
                    // Check max
                    if(config.isHouseCheckMax() && houseProp.getMax()!=0 && (houseData.getAvg() - houseProp.getMax()) >= (houseProp.getMax()*config.getHouseLogGap()/100)){
                        houseNotificationList.push(new HouseNotification(1, houseData, houseProp));
                    }
                    housePropList.put(houseData.getHouseUniqueId(), houseProp.addValue(houseData.getAvg()));
                }

                for(HouseholdData householdData : householdDataNeedSave){
                    HouseholdProp householdProp = householdPropList.getOrDefault(householdData.getHouseholdUniqueId(), new HouseholdProp(householdData.getHouseId(), householdData.getHouseholdId(), householdData.getGap()));
                    // Check min
                    if(config.isHouseholdCheckMin() && householdProp.getMin()!=0 && (householdProp.getMin() - householdData.getAvg()) <= (householdProp.getMin()*config.getHouseholdLogGap()/100)){
                        householdNotificationList.push(new HouseholdNotification(-1, householdData, householdProp));
                    }
                    // Check avg
                    if(config.isHouseholdCheckAvg() && householdProp.getAvg()!=0 && (householdData.getAvg() - householdProp.getAvg()) >= (householdProp.getAvg()*config.getHouseholdLogGap()/100)){
                        householdNotificationList.push(new HouseholdNotification(0, householdData, householdProp));
                    }
                    // Check max
                    if(config.isHouseholdCheckMax() && householdProp.getMax()!=0 && (householdData.getAvg() - householdProp.getMax()) >= (householdProp.getMax()*config.getHouseholdLogGap()/100)){
                        householdNotificationList.push(new HouseholdNotification(1, householdData, householdProp));
                    }
                    householdPropList.put(householdData.getHouseholdUniqueId(), householdProp.addValue(householdData.getAvg()));
                }
                
                // Save noti
                if(DB_store.pushHouseNotification(houseNotificationList, new File("./tmp/housenoti2db-" + gap + ".lck"))){
                    houseNotiUpdateFailCount=0;
                    // House noti saved
                    // Publish noti
                    if(config.isNotificationMQTT()){
                        MQTT_publisher.houseNotificationsPublish(houseNotificationList, config.getNotificationBrokerURL(), config.getMqttTopicPrefix(), new File("./tmp/housenoti2mqtt-"+gap+".lck"));
                    }
                }
                else if(++houseNotiUpdateFailCount>=3){
                    new File("./tmp/housenoti2db-" + gap + ".lck").delete();
                }

                if(DB_store.pushHouseholdNotification(householdNotificationList, new File("./tmp/householdnoti2db-" + gap + ".lck"))){
                    householdNotiUpdateFailCount=0;
                    // House noti pushed
                    // Publish noti
                    if(config.isNotificationMQTT()){
                        MQTT_publisher.householdNotificationsPublish(householdNotificationList, config.getNotificationBrokerURL(), config.getMqttTopicPrefix(), new File("./tmp/householdnoti2mqtt-"+gap+".lck"));
                    }
                }
                else if(++householdNotiUpdateFailCount>=3){
                    new File("./tmp/householdnoti2db-" + gap + ".lck").delete();
                }

                //Logging
                Long execTime = System.currentTimeMillis() - startExec;

                Stack<String> logs = new Stack<String>();
                logs.push(String.format("[Bolt_sum_%-3d] HouseData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n", gap, finalHouseDataSize, houseDataNeedSave.size(), houseDataNeedClean.size()));
                logs.push(String.format("[Bolt_sum_%-3d] HouseholdData | Total: %-10d | Need save: %-10d | Need clean: %-10d\n",gap, finalHouseholdDataSize, householdDataNeedSave.size(), householdDataNeedClean.size()));
                logs.push(String.format("[Bolt_sum_%-3d] Timeslice | Total: %-10d | Need clean: %-10d\n", gap, allData.size(), timesliceNeedClean.size()));
                logs.push(String.format("[Bolt_sum_%-3d] Notification | House: %-10d | Household: %-10d\n", gap, houseNotificationList.size(), householdNotificationList.size()));
                logs.push(String.format("[Bolt_sum_%-3d] Storing data execute time %.3f s\n", gap, (float) execTime/1000));
                MQTT_publisher.stormLogPublish(logs, config.getNotificationBrokerURL(), config.getMqttTopicPrefix(), new File("./tmp/bolt-sum-"+gap+"-log-publish.lck"));
                for(String data : logs){
                    System.out.println(data);
                }
                try {
                    FileWriter log = new FileWriter(new File("./tmp/bolt_sum_"+ gap +".tmp"), false);
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

                //Wipe unused data
                for(String timeslice : timesliceNeedClean){
                    allData.remove(timeslice);
                }

                for(HouseData houseData : houseDataNeedClean){
                    finalHouseDataList.get(houseData.getHouseId()).remove(houseData.getSliceId());
                }

                for(HouseholdData householdData : householdDataNeedClean) {
                    finalHouseholdDataList.get(householdData.getHouseholdUniqueId()).remove(householdData.getSliceId());
                }
                _collector.emit("trigger", new Values(triggerInterval, spoutProp));
                _collector.ack(tuple);
            }
            else if(tuple.getSourceStreamId().equals("data") && tuple.getValueByField("type").equals(DeviceData.class)){
                DeviceData tempData     = (DeviceData) tuple.getValueByField("data");

                HashMap<Integer, HashMap<Integer,HashMap<String, DeviceData> > > sliceData = allData.getOrDefault(tempData.getSliceId(), new HashMap<Integer, HashMap<Integer,HashMap<String, DeviceData> > >());
                HashMap<Integer,HashMap<String, DeviceData> > houseData = sliceData.getOrDefault(tempData.getHouseId(), new HashMap<Integer,HashMap<String, DeviceData> >());
                HashMap<String, DeviceData> householdData = houseData.getOrDefault(tempData.getHouseholdId(), new HashMap<String, DeviceData>());
                householdData.put(tempData.getUniqueId(), tempData);
                houseData.put(tempData.getHouseholdId(), householdData);
                sliceData.put(tempData.getHouseId(), houseData);
                allData.put(tempData.getSliceId(), sliceData);
                _collector.ack(tuple);
            }
            else{
                _collector.fail(tuple);
            }
        }
        catch (Exception ex){
            ex.printStackTrace();
            _collector.fail(tuple);
        }
    }
}