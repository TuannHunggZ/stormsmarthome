package com.storm.iotdata.storm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.google.gson.Gson;
import com.storm.iotdata.models.SpoutProp;
import com.storm.iotdata.models.StormConfig;

import org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class Spout_trigger extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private StormConfig config;

    public Spout_trigger(StormConfig config) {
        this.config = config;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void nextTuple() {
        try {
            System.out.println("[Spout-trigger] Sleeping for "+config.getUpdateInterval()+" second(s)");
            Thread.sleep(config.getUpdateInterval() * 1000);
            File tempFolder = new File("./tmp");
            File[] spoutLogs = tempFolder.listFiles();
            String name = String.format("%s@all-spout", config.getTopologyName());
            Boolean connect = true; 
            Float totalSpeed = Float.valueOf(0), loadSpeed = Float.valueOf(0);
            Long total = Long.valueOf(0), load = Long.valueOf(0), success = Long.valueOf(0), fail = Long.valueOf(0);
            Integer queue = 0;
            for (File Log : spoutLogs) {
                try {
                    if(Log.getName().contains("spout_data_log_")){
                        String raw = FileUtils.readFileToString(Log, StandardCharsets.UTF_8);
                        Gson gson = new Gson();
                        SpoutProp datas = gson.fromJson(raw, SpoutProp.class);
                        if(!datas.isConnect()){
                            connect = false;
                        }
                        totalSpeed+=datas.getTotalSpeed();
                        loadSpeed+=datas.getLoadSpeed();
                        total+=datas.getTotal();
                        load+=datas.getLoad();
                        queue+=datas.getQueue();
                        success+=datas.getSuccess();
                        fail+=datas.getFail();
                    }
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // System.gc();
            _collector.emit("trigger", new Values(config.getUpdateInterval(), new SpoutProp(name, connect, totalSpeed, loadSpeed, total, load, queue, success, fail))); // Trigger signal to write data to file after 1 min
        } catch (InterruptedException e) {
            e.printStackTrace();
            _collector.emit("trigger", new Values(config.getUpdateInterval(), new SpoutProp())); // Trigger signal to write data to file after 1 min
        } 
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("trigger", new Fields("trigger", "spoutProp"));
    }
}