package com.storm.forecast.models;

import java.sql.Time;
import java.time.YearMonth;
import java.util.Objects;

public class Timeslice{
    public String year;
    public String month;
    public String day;
    public Integer sliceIndex;
    public Integer sliceGap;

    public Timeslice() {
        super();
    }

    public Timeslice(Timeslice timeslice){
        this.year = timeslice.getYear();
        this.month = timeslice.getMonth();
        this.day = timeslice.getDay();
        this.sliceIndex = timeslice.getIndex();
        this.sliceGap = timeslice.getGap();
    }

    public Timeslice(String sliceId) {
        try{
            String[] sliceProp = sliceId.split("-");
            String year = sliceProp[0];
            String month = sliceProp[1];
            String day = sliceProp[2];
            Integer index = Integer.parseInt(sliceProp[3]);
            Integer gap = Integer.parseInt(sliceProp[4]);

            this.sliceGap = gap;
            Integer numSliceInDay = 24*60/this.sliceGap;
            Integer numDayInMonth = YearMonth.of(Integer.parseInt(year), Integer.parseInt(month)).lengthOfMonth();
            this.sliceIndex = ((index-1)%numSliceInDay)+1;
            this.day = String.format("%02d", ((Integer.parseInt(day) + Math.floorDiv(sliceIndex-1, numSliceInDay)-1)%numDayInMonth)+1);
            this.month = String.format("%02d", (((Integer.parseInt(month) + Math.floorDiv((Integer.parseInt(day) + Math.floorDiv(sliceIndex-1, numSliceInDay))-1, numDayInMonth)-1)%12)+1));
            if(Integer.parseInt(this.month)>12){
                this.year = String.format("%d", Integer.parseInt(year)+1);
            }
            else{
                this.year = year;
            }
        } catch (Exception ex) {
            throw ex;
        }
    }

    public Timeslice(String year, String month, String day, Integer index, Integer gap) {
        this.sliceGap = gap;
        Integer numSliceInDay = 24*60/this.sliceGap;
        Integer numDayInMonth = YearMonth.of(Integer.parseInt(year), Integer.parseInt(month)).lengthOfMonth();
        this.sliceIndex = (index%numSliceInDay);
        this.day = String.format("%02d", ((Integer.parseInt(day) + Math.floorDiv(sliceIndex, numSliceInDay) - 1)%numDayInMonth)+1);
        this.month = String.format("%02d", (((Integer.parseInt(month) + Math.floorDiv((Integer.parseInt(day) + Math.floorDiv(sliceIndex, numSliceInDay))-1, numDayInMonth)-1)%12)+1));
        if(Integer.parseInt(this.month)>12){
            this.year = String.format("%d", Integer.parseInt(year)+1);
        }
        else{
            this.year = year;
        }
    }

    public String getYear() {
        return this.year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return this.month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return this.day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public Integer getIndex() {
        return this.sliceIndex;
    }

    public void setIndex(Integer index) {
        this.sliceIndex = index;
    }

    public Integer getGap() {
        return this.sliceGap;
    }

    public void setGap(Integer gap) {
        this.sliceGap = gap;
    }

    public Timeslice year(String year) {
        this.year = year;
        return this;
    }

    public Timeslice month(String month) {
        this.month = month;
        return this;
    }

    public Timeslice day(String day) {
        this.day = day;
        return this;
    }

    public Timeslice index(Integer index) {
        this.sliceIndex = index;
        return this;
    }

    public Timeslice gap(Integer gap) {
        this.sliceGap = gap;
        return this;
    }

    public Boolean isSameTimeslice(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Timeslice)) {
            return false;
        }
        Timeslice timeslice = (Timeslice) o;
        return Objects.equals(year, timeslice.year) && Objects.equals(month, timeslice.month) && Objects.equals(day, timeslice.day) && Objects.equals(sliceIndex, timeslice.sliceIndex) && Objects.equals(sliceGap, timeslice.sliceGap);
    }

    public Timeslice getTimeslice(){
        return new Timeslice(year, month, day, sliceIndex, sliceGap);
    }

    public String getSliceId() {
        return year + "-" + month + "-" + day + "-" + sliceIndex + "-" + sliceGap;
    }

    public String getSliceName() {
        return year + "/" + month + "/" + day + " " +  String.format("%02d", Math.floorDiv((sliceIndex*sliceGap),60)) + ":" +  String.format("%02d", (sliceIndex*sliceGap)%60) + "->" +  String.format("%02d", Math.floorDiv(((sliceIndex+1)*sliceGap),60)) + ":" +  String.format("%02d", ((sliceIndex+1)*sliceGap)%60) ;
    }

    public String getDate() {
        return year + "/" + month + "/" + day;
    }

    public Timeslice getNextTimeslice(Integer num){
        return new Timeslice(this.year, this.month, this.day, this.sliceIndex + num, this.sliceGap);
    }

    public Timeslice getNextTimeslice(){
        return new Timeslice(this.year, this.month, this.day, this.sliceIndex + 1, this.sliceGap);
    }
}