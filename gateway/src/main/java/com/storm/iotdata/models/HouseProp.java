package com.storm.iotdata.models;

import java.io.Serializable;
import java.util.Objects;

public class HouseProp implements Serializable{
    public int houseId;
    public int sliceGap;
    public Double min;
    public Double max;
    public Double avg;
    public Double count;
    public Long lastUpdate;
    public boolean saved = false;

    public HouseProp(int houseId, int sliceGap){
        this.houseId = houseId;
        this.sliceGap = sliceGap;
        this.min = Double.valueOf(0);
        this.max = Double.valueOf(0);
        this.avg = Double.valueOf(0);
        this.count = Double.valueOf(0);
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public HouseProp(int houseId, int sliceGap, Double min, Double max, Double avg){
        this.houseId = houseId;
        this.sliceGap = sliceGap;
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.count = Double.valueOf(1);
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public HouseProp(int houseId, int sliceGap, Double min, Double max, Double avg, Double count, Boolean saved){
        this.houseId = houseId;
        this.sliceGap = sliceGap;
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.count = count;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = saved;
    }

    public int getHouseId() {
        return this.houseId;
    }

    public void setHouseId(int houseId) {
        this.houseId = houseId;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public int getSliceGap() {
        return this.sliceGap;
    }

    public void setSliceGap(int sliceGap) {
        this.sliceGap = sliceGap;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public Double getMin() {
        return this.min;
    }

    public void setMin(Double min) {
        this.min = min;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public Double getMax() {
        return this.max;
    }

    public void setMax(Double max) {
        this.max = max;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public Double getAvg() {
        return this.avg;
    }

    public void setAvg(Double avg) {
        this.avg = avg;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public Double getCount() {
        return this.count;
    }

    public void setCount(Double count) {
        this.count = count;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public Long getLastUpdate() {
        return this.lastUpdate;
    }

    public void setLastUpdate(Long lastUpdate) {
        this.lastUpdate = lastUpdate;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
    }

    public boolean isSaved() {
        return this.saved;
    }

    public HouseProp houseId(int houseId) {
        this.houseId = houseId;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public HouseProp sliceGap(int sliceGap) {
        this.sliceGap = sliceGap;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public HouseProp min(Double min) {
        this.min = min;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public HouseProp max(Double max) {
        this.max = max;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public HouseProp avg(Double avg) {
        this.avg = avg;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public HouseProp count(Double count) {
        this.count = count;
        this.lastUpdate = System.currentTimeMillis();
        this.saved = false;
        return this;
    }

    public HouseProp addValue(Double value) {
        if(value != 0){
            this.avg = (this.avg*this.count + value)/(++this.count);
            if(value<this.getMin()){
                this.setMin(value);
            }
            else if(value>this.getMax()){
                this.setMax(value);
            }
        }
        return this;
    }

    public HouseProp lastUpdate() {
        this.lastUpdate = System.currentTimeMillis();
        return this;
    }

    public HouseProp save() {
        this.saved = true;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof HouseProp)) {
            return false;
        }
        HouseProp houseProp = (HouseProp) o;
        return houseId == houseProp.houseId && sliceGap == houseProp.sliceGap && Objects.equals(min, houseProp.min) && Objects.equals(max, houseProp.max) && Objects.equals(avg, houseProp.avg) && Objects.equals(count, houseProp.count) && Objects.equals(lastUpdate, houseProp.lastUpdate) && saved == houseProp.saved;
    }

    @Override
    public int hashCode() {
        return Objects.hash(houseId, sliceGap, min, max, avg, count, lastUpdate, saved);
    }

    @Override
    public String toString() {
        return "{" +
            " houseId='" + getHouseId() + "'" +
            ", sliceGap='" + getSliceGap() + "'" +
            ", min='" + getMin() + "'" +
            ", max='" + getMax() + "'" +
            ", avg='" + getAvg() + "'" +
            ", count='" + getCount() + "'" +
            ", lastUpdate='" + getLastUpdate() + "'" +
            ", saved='" + isSaved() + "'" +
            "}";
    }

    public String getUniqueId(){
        return String.format("%d-%d", houseId, sliceGap);
    }

    public String getHouseUniqueId(){
        return String.valueOf(getHouseId());
    }
}