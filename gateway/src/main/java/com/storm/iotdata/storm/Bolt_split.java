
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.storm.iotdata.storm;

import java.util.Date;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.storm.iotdata.models.*;

/**
 *
 * @author hiiamlala
 */
public class Bolt_split extends BaseRichBolt {
    private StormConfig config;
    private int window;
    public Bolt_split(int window, StormConfig config) {
        this.window = window;
        this.config = config;
    }

    // output collector
    private OutputCollector _collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("data", new Fields("houseId", "householdId", "deviceId", "year", "month", "day", "sliceIndex", "value"));
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext tc, OutputCollector oc) {
        _collector = oc;
    }

    @Override
    public void execute(Tuple tuple) {
        try{
            if(tuple.getSourceStreamId().equals("data")){
                Integer houseId     = Integer.parseInt((String)tuple.getValueByField("houseId"));
                Integer householdId = Integer.parseInt((String)tuple.getValueByField("householdId"));
                Integer plugId      = Integer.parseInt((String)tuple.getValueByField("plugId"));
                Long    timestamp    = Long.parseLong((String)tuple.getValueByField("timestamp"));
                Double  value        = Double.parseDouble((String)tuple.getValueByField("value"));
                // Integer property     = Integer.parseInt((String)tuple.getValueByField("property"));
                // Timestamp stamp = new Timestamp(timestamp);
                Date date = new Date(timestamp*1000);
                String year = Integer.toString(1900 + date.getYear());;
                String month = String.format("%02d", (1+date.getMonth()));
                String day = String.format("%02d", date.getDate()) ;
                Long time = (date.getTime()%86400000);
                int sliceIndex = (int) Math.floorDiv(time,(window*60000));
                _collector.emit("data", new Values(houseId, householdId, plugId, year, month, day, sliceIndex, value));
                _collector.ack(tuple);
            }
            else{
                _collector.fail(tuple);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            _collector.fail(tuple);
        }
    }
}