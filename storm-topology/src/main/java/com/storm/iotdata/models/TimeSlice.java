package com.storm.iotdata.models;

import java.io.Serializable;

public class TimeSlice implements Serializable {
    public String year;
    public String month;
    public String day;
    public Integer sliceIndex;
    public Integer sliceGap;

    public TimeSlice() {
    }

    public TimeSlice(String year, String month, String day, Integer sliceIndex, Integer sliceGap) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.sliceIndex = sliceIndex;
        this.sliceGap = sliceGap;
    }

    public String getYear() {
        return this.year;
    }

    public String getMonth() {
        return this.month;
    }

    public String getDay() {
        return this.day;
    }

    public Integer getSliceIndex() {
        return this.sliceIndex;
    }

    public Integer getSliceGap() {
        return this.sliceGap;
    }

    public String getSliceId() {
        return year + "-" + month + "-" + day + "-" + sliceIndex + "-" + sliceGap;
    }
}