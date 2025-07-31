package com.storm.forecast.models;

import java.io.Serializable;
import java.util.Objects;

public class DeviceData extends Timeslice implements Serializable {

    public Integer houseId;
    public Integer householdId;
    public Integer deviceId;
    public Double value = Double.valueOf(0);
    public Double count;
    public Long lastUpdate;
    public Boolean saved=false;

    public DeviceData() {
        super();
    }


    public DeviceData(Integer houseId, Integer householdId, Integer deviceId, Timeslice timeslice, Double value, Double count) {
        super(timeslice);
        this.houseId=houseId;
        this.householdId=householdId;
        this.deviceId=deviceId;
        this.value=value;
        this.count=count;
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
    }

    public DeviceData(Integer houseId, Integer householdId, Integer deviceId, Timeslice timeslice, Double avg) {
        super(timeslice);
        this.houseId=houseId;
        this.householdId=householdId;
        this.deviceId=deviceId;
        this.value=avg;
        this.count=Double.valueOf(1);
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
    }

    public DeviceData(Integer houseId, Integer householdId, Integer deviceId, Timeslice timeslice, Double value, Double count, Boolean saved) {
        super(timeslice);
        this.houseId=houseId;
        this.householdId=householdId;
        this.deviceId=deviceId;
        this.value=value;
        this.count=count;
        this.lastUpdate=System.currentTimeMillis();
        this.saved=saved;
    }

    public DeviceData(Integer houseId, Integer householdId, Integer deviceId, String year, String month, String day, Integer sliceIndex, Integer sliceGap, Double avg) {
        super(year, month, day, sliceIndex, sliceGap);
        this.houseId=houseId;
        this.householdId=householdId;
        this.deviceId=deviceId;
        this.value=avg;
        this.count=Double.valueOf(1);
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
    }

    public DeviceData(Integer houseId, Integer householdId, Integer deviceId, String year, String month, String day, Integer sliceIndex, Integer sliceGap, Double value, Double count, Boolean saved) {
        super(year, month, day, sliceIndex, sliceGap);
        this.houseId=houseId;
        this.householdId=householdId;
        this.deviceId=deviceId;
        this.value=value;
        this.count=count;
        this.lastUpdate=System.currentTimeMillis();
        this.saved=saved;
    }

    public DeviceData(Integer houseId, Integer householdId, Integer deviceId, String year, String month, String day, Integer sliceIndex, Integer sliceGap) {
        super(year, month, day, sliceIndex, sliceGap);
        this.houseId=houseId;
        this.householdId=householdId;
        this.deviceId=deviceId;
        this.value=Double.valueOf(0);
        this.count=Double.valueOf(0);
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
    }

    public Integer getHouseId() {
        return this.houseId;
    }

    public void setHouseId(Integer houseId) {
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.houseId=houseId;
    }

    public Integer getHouseholdId() {
        return this.householdId;
    }

    public void setHouseholdId(Integer householdId) {
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.householdId=householdId;
    }

    public Integer getDeviceId() {
        return this.deviceId;
    }

    public void setDeviceId(Integer deviceId) {
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.deviceId=deviceId;
    }

    public DeviceData avg(Double avg) {
        this.lastUpdate=System.currentTimeMillis();
        this.count=Double.valueOf(1);
        this.value=avg;
        return this;
    }

    public Double getValue() {
        return this.value;
    }

    public void setValue(Double value) {
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.value=value;
    }

    public Double getCount() {
        return this.count;
    }

    public void setCount(Double count) {
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.count=count;
    }

    public long getLastUpdate() {
        return this.lastUpdate;
    }

    public Double getAvg() {
        if(this.count==0){
            return  Double.valueOf(0);
        }
        return this.value/this.count;
    }

    public DeviceData houseId(Integer houseId) {
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.houseId=houseId;
        return this;
    }

    public DeviceData householdId(Integer householdId) {
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.householdId=householdId;
        return this;
    }

    public DeviceData deviceId(Integer deviceId) {
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.deviceId=deviceId;
        return this;
    }

    public DeviceData saved(Boolean saved) {
        this.saved=saved;
        return this;
    }

    public Boolean isSaved() {
        return this.saved;
    }

    public DeviceData save() {
        this.saved=true;
        return this;
    }

    public DeviceData value(Double value) {
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.value=value;
        return this;
    }

    public DeviceData increaseValue(Double value){
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.value+=value;
        this.count++;
        return this;
    }

    public DeviceData count(Double count) {
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.count=count;
        return this;
    }

    @Override
    public String toString() {
        return "{" +
            " houseId='" + getHouseId() + "'" +
            ", householdId='" + getHouseholdId() + "'" +
            ", deviceId='" + getDeviceId() + "'" +
            ", year='" + getYear() + "'" +
            ", month='" + getMonth() + "'" +
            ", day='" + getDay() + "'" +
            ", sliceIndex='" + getIndex() + "'" +
            ", sliceGap='" + getGap() + "'" +
            ", value='" + getValue() + "'" +
            ", count='" + getCount() + "'" +
            ", lastUpdate='" + getLastUpdate() + "'" +
            ", saved='" + isSaved() + "'" +
            "}";
    }

    public String getUniqueId(){
        return String.format("%d-%d-%d-%s-%s-%s-%d-%d", houseId, householdId, deviceId, year, month, day, sliceGap, sliceIndex);
    }

	public String getDeviceUniqueId() {
		return String.format("%d-%d-%d", houseId, householdId, deviceId);
    }
    
    public String getHouseholdUniqueId() {
        return String.format("%d-%d", houseId, householdId);
    }
    
}