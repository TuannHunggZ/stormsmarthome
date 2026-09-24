package com.storm.iotdata.models;

public class PlugData extends TimeSlice {
    public Integer houseId;
    public Integer householdId;
    public Integer plugId;
    public Double value;
    public Double count;
    public Boolean saved=false;

    public PlugData() {
        super();
    }

    public PlugData(Integer houseId, Integer householdId, Integer plugId, String year, String month, String day, Integer sliceIndex, Integer sliceGap) {
        super(year, month, day, sliceIndex, sliceGap);
        this.houseId = houseId;
        this.householdId = householdId;
        this.plugId = plugId;
        this.value=Double.valueOf(0);
        this.count=Double.valueOf(0);
        this.saved=false;
    }

    public Integer getHouseId() {
        return this.houseId;
    }

    public Integer getHouseholdId() {
        return this.householdId;
    }

    public Integer getPlugId() {
        return this.plugId;
    }

    public Double getValue() {
        return this.value;
    }

    public Double getCount() {
        return this.count;
    }

    public Double getAvg() {
        return this.count > 0 ? this.value / this.count : 0;
    }

    public Boolean isSaved() {
        return this.saved;
    }

    public PlugData save() {
        this.saved=true;
        return this;
    }

    public String getUniqueId(){
        return String.format("%d-%d-%d-%s-%s-%s-%d-%d", houseId, householdId, plugId, year, month, day, sliceGap, sliceIndex);
    }

    public String getPlugUniqueId() {
		return String.format("%d-%d-%d", houseId, householdId, plugId);
    }

    public PlugData increaseValue(Double value){
        this.saved = false;
        this.value += value;
        this.count++;
        return this;
    }
    
}