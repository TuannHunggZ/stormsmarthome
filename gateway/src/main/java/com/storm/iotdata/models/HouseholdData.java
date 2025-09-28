package com.storm.iotdata.models;

import java.io.Serializable;

public class HouseholdData extends Timeslice implements Serializable {

    public Integer houseId;
    public Integer householdId;
    public Double value;
    public Long lastUpdate;
    public Boolean saved = false;

    public HouseholdData(Integer houseId, Integer householdId, String timeslice, Double value) {
        super(timeslice);
        this.houseId = houseId;
        this.householdId = householdId;
        this.value = value;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public HouseholdData(Integer houseId, Integer householdId, String timeslice, Double value, Boolean saved) {
        super(timeslice);
        this.houseId = houseId;
        this.householdId = householdId;
        this.value = value;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = saved;
    }

    public HouseholdData(Integer houseId, Integer householdId, Timeslice timeslice, Double value) {
        super(timeslice);
        this.houseId = houseId;
        this.householdId = householdId;
        this.value = value;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public HouseholdData(Integer houseId, Integer householdId, Timeslice timeslice, Double value, Boolean saved) {
        super(timeslice);
        this.houseId = houseId;
        this.householdId = householdId;
        this.value = value;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = saved;
    }

    public HouseholdData(Integer houseId, Integer householdId, String year, String month, String day, Integer sliceIndex, Integer sliceGap) {
        super(year, month, day, sliceIndex, sliceGap);
        this.houseId = houseId;
        this.householdId = householdId;
        this.value = Double.valueOf(0);
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public HouseholdData(Integer houseId, Integer householdId, String year, String month, String day, Integer sliceIndex, Integer sliceGap, Double value) {
        super(year, month, day, sliceIndex, sliceGap);
        this.houseId = houseId;
        this.householdId = householdId;
        this.value = value;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public HouseholdData(Integer houseId, Integer householdId, String year, String month, String day, Integer sliceIndex, Integer sliceGap, Double value, Long lastUpdate, Boolean saved) {
        super(year, month, day, sliceIndex, sliceGap);
        this.houseId = houseId;
        this.householdId = householdId;
        this.value = value;
        this.lastUpdate = lastUpdate;
        this.saved = saved;
    }

    public Integer getHouseId() {
        return this.houseId;
    }

    public void setHouseId(Integer houseId) {
        if(this.houseId != houseId){
            this.houseId = houseId;
            this.saved = false;
            setLastUpdate();
        }
    }

    public Integer getHouseholdId() {
        return this.householdId;
    }

    public void setHouseholdId(Integer householdId) {
        if(this.householdId != householdId){
            this.householdId = householdId;
            this.saved = false;
            setLastUpdate();
        }
    }

    public Double getValue() {
        return this.value;
    }

    public void setValue(Double value) {
        if(!this.value.equals(value)){
            this.value = value;
            this.saved = false;
            setLastUpdate();
        }
    }

    public Long getLastUpdate() {
        return this.lastUpdate;
    }

    public void setLastUpdate() {
        this.lastUpdate = System.currentTimeMillis();
    }

    public Boolean isSaved() {
        return this.saved;
    }

    public HouseholdData houseId(Integer houseId) {
        this.setHouseId(houseId);
        return this;
    }

    public HouseholdData householdId(Integer householdId) {
        this.setHouseId(householdId);
        return this;
    }

    public HouseholdData value(Double value) {
        this.setValue(value);
        return this;
    }

    public HouseholdData save() {
        this.saved = true;
        return this;
    }

    public HouseholdData increaseValue(Double value){
        this.lastUpdate=System.currentTimeMillis();
        this.saved=false;
        this.value+=value;
        return this;
    }

    @Override
    public String toString() {
        return "{" +
            " houseId='" + getHouseId() + "'" +
            ", householdId='" + getHouseholdId() + "'" +
            ", year='" + getYear() + "'" +
            ", month='" + getMonth() + "'" +
            ", day='" + getDay() + "'" +
            ", sliceIndex='" + getIndex() + "'" +
            ", sliceGap='" + getGap() + "'" +
            ", value='" + getValue() + "'" +
            ", lastUpdate='" + getLastUpdate() + "'" +
            ", saved='" + isSaved() + "'" +
            "}";
    }

    public String getUniqueId() {
        return String.format("%d-%d-%s-%s-%s-%d-%d", houseId, householdId, year, month, day, sliceGap, sliceIndex);
    }

    public String getHouseholdUniqueId() {
        return String.format("%d-%d", houseId, householdId);
    }

    public Double getAvg() {
        return getValue();
    }
}
