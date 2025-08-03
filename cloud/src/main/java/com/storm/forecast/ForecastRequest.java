package com.storm.forecast;

import java.util.HashMap;
import com.storm.forecast.models.*;

public class ForecastRequest {
    private HashMap<String, HouseData> houseData;
    private HashMap<String, HouseholdData> householdData;
    private HashMap<String, DeviceData> deviceData;
    private int gap;
    private String notificationBrokerURL;
    private String mqttTopicPrefix;

    public HashMap<String, HouseData> getHouseData() {
        return houseData;
    }

    public void setHouseData(HashMap<String, HouseData> houseData) {
        this.houseData = houseData;
    }

    public HashMap<String, HouseholdData> getHouseholdData() {
        return householdData;
    }

    public void setHouseholdData(HashMap<String, HouseholdData> householdData) {
        this.householdData = householdData;
    }

    public HashMap<String, DeviceData> getDeviceData() {
        return deviceData;
    }

    public void setDeviceData(HashMap<String, DeviceData> deviceData) {
        this.deviceData = deviceData;
    }

    public int getGap() {
        return gap;
    }

    public void setGap(int gap) {
        this.gap = gap;
    }

    public String getNotificationBrokerURL() {
        return notificationBrokerURL;
    }

    public void setNotificationBrokerURL(String notificationBrokerURL) {
        this.notificationBrokerURL = notificationBrokerURL;
    }

    public String getMqttTopicPrefix() {
        return mqttTopicPrefix;
    }

    public void setMqttTopicPrefix(String mqttTopicPrefix) {
        this.mqttTopicPrefix = mqttTopicPrefix;
    }
}