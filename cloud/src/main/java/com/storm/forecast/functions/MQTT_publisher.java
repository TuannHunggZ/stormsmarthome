package com.storm.forecast.functions;

import java.io.File;
import java.util.Stack;
import java.util.UUID;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.storm.forecast.models.*;

public class MQTT_publisher {   
    public static void stormLogPublish(Stack<String> dataList, String brokerURL, String topicPrefix, File locker) {
        new StormLogPublisher(dataList, brokerURL, topicPrefix, locker).start();
    }
}

class StormLogPublisher extends Thread {
    String brokerURL = "tcp://mqtt-broker:1883";
    String stormLogTopic = "%sstorm-log";
    String topicPrefix = "";
    File locker;
    Stack<String> dataList;

    public StormLogPublisher(Stack<String> dataList, String brokerURL, String topicPrefix, File locker) {
        this.dataList = dataList;
        this.brokerURL = brokerURL;
        this.topicPrefix = topicPrefix;
        this.locker = locker;
    }

    @Override
    public void run() {
        Long start = System.currentTimeMillis();
        String publisherId = UUID.randomUUID().toString();
        try {
            locker.createNewFile();
            IMqttClient publisher = new MqttClient(brokerURL, publisherId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            publisher.connect(options);
            if(publisher.isConnected())
            for(String log : dataList){
                byte[] payload = log.getBytes();
                MqttMessage msg = new MqttMessage(payload);
                msg.setQos(0);
                msg.setRetained(true);
                publisher.publish(String.format(stormLogTopic, topicPrefix), msg);
            }
            publisher.disconnect();
            publisher.close();
            System.out.printf("[Storm Log Publisher] MQTT Publisher took %.2f s\n", (float) (System.currentTimeMillis() - start) / 1000);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        locker.delete();
    }
}