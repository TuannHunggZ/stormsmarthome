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
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.storm.iotdata.models.SpoutProp;
import com.storm.iotdata.models.StormConfig;

import org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;

public class Spout_data implements MqttCallback, IRichSpout {

    private SpoutOutputCollector _collector;
    LinkedBlockingQueue<String> messages;
    Long total = Long.valueOf(0);
    Long success = Long.valueOf(0);
    Long fail = Long.valueOf(0);
    Long speed = Long.valueOf(0);
    Long load = Long.valueOf(0);
    Long totalLoad = Long.valueOf(0);
    Long last = System.currentTimeMillis();
    MqttClient client;
    String clientId = "";
    String topic = "iot-data";
    String logTopic = "%sspout-log";
    StormConfig config;

    public Spout_data(StormConfig config, String topic) {
        this.config = config;
        this.topic = topic;
        clientId = new String(config.getTopologyName() + "@" + topic);
        messages = new LinkedBlockingQueue<String>();
        if (!(new File("tmp").isDirectory())) {
            new File("tmp").mkdir();
        } else {
            try {
                FileUtils.cleanDirectory(new File("tmp"));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public void messageArrived(String topic, MqttMessage message) throws Exception {
        messages.add(message.toString());
        total++;
        speed++;
    }

    public void connectionLost(Throwable cause) {
        try {
            log();
            System.out.println("[Spout-data-" + topic + "] Lost connection with broker. Trying to reconnect in 10s");
            Thread.sleep(10000);
            client.reconnect();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        System.out.println("[Spout-data-" + topic + "] Connecting to broker (" + config.getSpoutBrokerURL() + ")..");
        initMQTTClient();
    }

    public void close() {
        try {
            client.disconnect();
        } catch (MqttException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        new File("/tmp/spout_log_" + topic + ".tmp").delete();
    }

    public void activate() {
        if (client.isConnected()) {
            try {
                client.subscribe(topic);
                System.out.println("[Spout-data-" + topic + "] Subscribed to topic " + topic + ".");
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            try {
                client.connect();
                client.subscribe(topic);
            } catch (MqttSecurityException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                initMQTTClient();
                e.printStackTrace();
            }
        }
    }

    public void deactivate() {
        if (client.isConnected()) {
            try {
                client.unsubscribe(topic);
                System.out.println("[Spout-data-" + topic + "] Unsubscribed topic " + topic + ".");
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            try {
                client.reconnect();
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public void nextTuple() {
        while (!messages.isEmpty()) {
            try {
                String message = messages.poll();
                String[] metric = message.split(",");
                if (Integer.parseInt(metric[3]) == 1) { // On prend juste les loads
                    _collector.emit("data", new Values(metric[1], metric[2], metric[3], metric[4], metric[5], metric[6]),
                            message);
                    load++;
                    totalLoad++;
                }
                if (System.currentTimeMillis() - last > 10000) {
                    log();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (System.currentTimeMillis() - last > 10000) {
            log();
        }
    }

    public void log() {
        String log = new SpoutProp(clientId, client.isConnected(),
                (float) (speed * 1000 / (System.currentTimeMillis() - last)),
                (float) (load * 1000 / (System.currentTimeMillis() - last)), total, totalLoad, messages.size(), success,
                fail).toString();
        new SpoutDataLogger(client, String.format(logTopic, config.getMqttTopicPrefix()),
                new File("tmp/spout_data_log_" + topic + ".tmp"), log).start();
        speed = Long.valueOf(0);
        load = Long.valueOf(0);
        last = System.currentTimeMillis();
    }

    public void ack(Object msgId) {
        success++;
    }

    public void fail(Object msgId) {
        fail++;
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /* uses default stream id */
        declarer.declareStream("data", new Fields("timestamp", "value", "property", "plugId", "householdId", "houseId"));
    }

    public void initMQTTClient() {
        System.out.println("[Spout-data-" + topic + "] Connecting to broker (" + config.getSpoutBrokerURL() + ")..");
        try {
            if (client!=null) {
                client.close(true);
            }
            client = new MqttClient(config.getSpoutBrokerURL(), clientId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setConnectionTimeout(10);
            client.connect(options);
            System.out.println("[Spout-data-" + topic + "] Connected to broker (" + config.getSpoutBrokerURL() + ").");
            client.setCallback(this);
        } catch (MqttException e) {
            e.printStackTrace();
            try {
                Thread.sleep(10000);
                System.out.println("[Spout-data-" + topic + "] Waiting 10s before retry to connect to MQTT Broker (" + config.getSpoutBrokerURL() + ").");
                initMQTTClient();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
    }
}

class SpoutDataLogger extends Thread{
    String log;
    MqttClient client;
    String logTopic;
    File logFile;

    public SpoutDataLogger(MqttClient client,String logTopic, File logFile, String log){
        this.client = client;
        this.logTopic = logTopic;
        this.logFile = logFile;
        this.log = log;
    }

    @Override
    public void run(){
        try {
            FileWriter logWriter = new FileWriter(logFile, false);
            PrintWriter pwOb = new PrintWriter(logWriter , false);
            pwOb.flush();
            logWriter.write(log);
            pwOb.close();
            logWriter.close();
            byte[] payLoad = log.getBytes();
            client.publish(logTopic, payLoad, 0, true);
        } catch (MqttException ex) {
            ex.printStackTrace();
            try {
                client.reconnect();
            } catch (MqttException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
