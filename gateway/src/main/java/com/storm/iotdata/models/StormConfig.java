package com.storm.iotdata.models;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.Arrays;
import java.util.List;

import com.google.gson.Gson;

import org.yaml.snakeyaml.Yaml;

public class StormConfig implements Serializable{
    //default value config
    private boolean cleanDatabase = false;
    private Integer updateInterval = 1;
    private String topologyName = "iot-smarthome";
    private String stormBrokerURL = "tcp://mqtt-broker:1883";
    private List<String> stormTopicList = Arrays.asList(new String[] { "iot-data" });
    private List<Integer> stormWindowList = Arrays.asList(new Integer[] { 1, 5, 10, 15, 20, 30, 60, 120 });
    private boolean notificationMQTT = true;
    private String notificationBrokerURL = "tcp://mqtt-broker:1883";
    private String mqttTopicPrefix = "";
    private int houseLogGap = 20;
    private boolean houseCheckMin = false;
    private boolean houseCheckAvg = true;
    private boolean houseCheckMax = true;
    private int householdLogGap = 20;
    private boolean householdCheckMin = false;
    private boolean householdCheckAvg = true;
    private boolean householdCheckMax = true;
    private int deviceLogGap = 20;
    private boolean deviceCheckMin = false;
    private boolean deviceCheckAvg = true;
    private boolean deviceCheckMax = true;

    public StormConfig(){
        Yaml yaml = new Yaml();
        InputStream inputStream = StormConfig.class.getClassLoader().getResourceAsStream("config/conf.yaml");
        Map<String, Object> obj = yaml.load(inputStream);
        this.cleanDatabase = (Boolean) obj.getOrDefault("database.clean", this.cleanDatabase);
        this.updateInterval = (Integer) obj.getOrDefault("database.updateInterval", this.updateInterval);
        this.topologyName = (String) obj.getOrDefault("storm.topologyName", this.topologyName);
        this.stormBrokerURL = (String) obj.getOrDefault("spout.brokerURL", this.stormBrokerURL);
        this.stormTopicList = (List<String>) obj.getOrDefault("spout.topicList", this.stormTopicList);
        this.stormWindowList = (List<Integer>) obj.getOrDefault("iot analytics.windowList", this.stormWindowList);
        this.notificationMQTT = (Boolean) obj.getOrDefault("notification.publishMQTT", this.notificationMQTT);
        this.notificationBrokerURL = (String) obj.getOrDefault("notification.brokerURL", this.notificationBrokerURL);
        this.houseLogGap = (int) obj.getOrDefault("notification.house.logGap", this.houseLogGap);
        this.houseCheckMin = (Boolean) obj.getOrDefault("notification.house.checkMin", this.houseCheckMin);
        this.houseCheckAvg = (Boolean) obj.getOrDefault("notification.house.checkAvg", this.houseCheckAvg);
        this.houseCheckMax = (Boolean) obj.getOrDefault("notification.house.checkMax", this.houseCheckMax);
        this.householdLogGap = (int) obj.getOrDefault("notification.household.logGap", this.householdLogGap);
        this.householdCheckMin = (Boolean) obj.getOrDefault("notification.household.checkMin", this.householdCheckMin);
        this.householdCheckAvg = (Boolean) obj.getOrDefault("notification.household.checkAvg", this.householdCheckAvg);
        this.householdCheckMax = (Boolean) obj.getOrDefault("notification.household.checkMax", this.householdCheckMax);
        this.deviceLogGap = (int) obj.getOrDefault("notification.device.logGap", this.deviceLogGap);
        this.deviceCheckMin = (Boolean) obj.getOrDefault("notification.device.checkMin", this.deviceCheckMin);
        this.deviceCheckAvg = (Boolean) obj.getOrDefault("notification.device.checkAvg", this.deviceCheckAvg);
        this.deviceCheckMax = (Boolean) obj.getOrDefault("notification.device.checkMax", this.deviceCheckMax);
    }

    public boolean isCleanDatabase() {
        return this.cleanDatabase;
    }

    public boolean getCleanDatabase() {
        return this.cleanDatabase;
    }

    public void setCleanDatabase(boolean cleanDatabase) {
        this.cleanDatabase = cleanDatabase;
    }

    public Integer getUpdateInterval() {
        return this.updateInterval;
    }

    public void setUpdateInterval(Integer updateInterval) {
        this.updateInterval = updateInterval;
    }

    public String getTopologyName() {
        return this.topologyName;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    public String getSpoutBrokerURL() {
        return this.stormBrokerURL;
    }

    public void setSpoutBrokerURL(String spoutBrokerURL) {
        this.stormBrokerURL = spoutBrokerURL;
    }

    public List<String> getSpoutTopicList() {
        return this.stormTopicList;
    }

    public void setSpoutTopicList(List<String> spoutTopicList) {
        this.stormTopicList = spoutTopicList;
    }

    public List<Integer> getWindowList() {
        return this.stormWindowList;
    }

    public void setWindowList(List<Integer> windowList) {
        this.stormWindowList = windowList;
    }

    public boolean isNotificationMQTT() {
        return this.notificationMQTT;
    }

    public void setNotificationMQTT(boolean notificationMQTT) {
        this.notificationMQTT = notificationMQTT;
    }

    public String getNotificationBrokerURL() {
        return this.notificationBrokerURL;
    }

    public void setNotificationBrokerURL(String notificationBrokerURL) {
        this.notificationBrokerURL = notificationBrokerURL;
    }


    public String getMqttTopicPrefix() {
        return this.mqttTopicPrefix;
    }

    public void setMqttTopicPrefix(String mqttTopicPrefix) {
        this.mqttTopicPrefix = mqttTopicPrefix;
    }


    public int getHouseLogGap() {
        return this.houseLogGap;
    }

    public void setHouseLogGap(int houseLogGap) {
        this.houseLogGap = houseLogGap;
    }

    public boolean isHouseCheckMin() {
        return this.houseCheckMin;
    }

    public boolean getHouseCheckMin() {
        return this.houseCheckMin;
    }

    public void setHouseCheckMin(boolean houseCheckMin) {
        this.houseCheckMin = houseCheckMin;
    }

    public boolean isHouseCheckAvg() {
        return this.houseCheckAvg;
    }

    public boolean getHouseCheckAvg() {
        return this.houseCheckAvg;
    }

    public void setHouseCheckAvg(boolean houseCheckAvg) {
        this.houseCheckAvg = houseCheckAvg;
    }

    public boolean isHouseCheckMax() {
        return this.houseCheckMax;
    }

    public boolean getHouseCheckMax() {
        return this.houseCheckMax;
    }

    public void setHouseCheckMax(boolean houseCheckMax) {
        this.houseCheckMax = houseCheckMax;
    }

    public int getHouseholdLogGap() {
        return this.householdLogGap;
    }

    public void setHouseholdLogGap(int householdLogGap) {
        this.householdLogGap = householdLogGap;
    }

    public boolean isHouseholdCheckMin() {
        return this.householdCheckMin;
    }

    public boolean getHouseholdCheckMin() {
        return this.householdCheckMin;
    }

    public void setHouseholdCheckMin(boolean householdCheckMin) {
        this.householdCheckMin = householdCheckMin;
    }

    public boolean isHouseholdCheckAvg() {
        return this.householdCheckAvg;
    }

    public boolean getHouseholdCheckAvg() {
        return this.householdCheckAvg;
    }

    public void setHouseholdCheckAvg(boolean householdCheckAvg) {
        this.householdCheckAvg = householdCheckAvg;
    }

    public boolean isHouseholdCheckMax() {
        return this.householdCheckMax;
    }

    public boolean getHouseholdCheckMax() {
        return this.householdCheckMax;
    }

    public void setHouseholdCheckMax(boolean householdCheckMax) {
        this.householdCheckMax = householdCheckMax;
    }
    
    public int getDeviceLogGap() {
        return this.deviceLogGap;
    }

    public void setDeviceLogGap(int deviceLogGap) {
        this.deviceLogGap = deviceLogGap;
    }

    public boolean isDeviceCheckMin() {
        return this.deviceCheckMin;
    }

    public boolean getDeviceCheckMin() {
        return this.deviceCheckMin;
    }

    public void setDeviceCheckMin(boolean deviceCheckMin) {
        this.deviceCheckMin = deviceCheckMin;
    }

    public boolean isDeviceCheckAvg() {
        return this.deviceCheckAvg;
    }

    public boolean getDeviceCheckAvg() {
        return this.deviceCheckAvg;
    }

    public void setDeviceCheckAvg(boolean deviceCheckAvg) {
        this.deviceCheckAvg = deviceCheckAvg;
    }

    public boolean isDeviceCheckMax() {
        return this.deviceCheckMax;
    }

    public boolean getDeviceCheckMax() {
        return this.deviceCheckMax;
    }

    public void setDeviceCheckMax(boolean deviceCheckMax) {
        this.deviceCheckMax = deviceCheckMax;
    }

    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
