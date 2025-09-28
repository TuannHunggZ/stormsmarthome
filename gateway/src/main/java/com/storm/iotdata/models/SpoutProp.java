package com.storm.iotdata.models;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.google.gson.Gson;

public class SpoutProp {
    private String name;
    private Boolean connect;
    private Float totalSpeed = Float.valueOf(0);
    private Float loadSpeed = Float.valueOf(0);
    private Long total = Long.valueOf(0);
    private Long load = Long.valueOf(0);
    private Integer queue = 0;
    private Long success = Long.valueOf(0);
    private Long fail = Long.valueOf(0);
    private String lastUpdate = getCurrentISOString();

    public SpoutProp(){
        this.name = "ERROR";
        this.connect = false;
    }

    public SpoutProp(String name, Boolean connect, Float totalSpeed, Float loadSpeed, Long total, Long load, Integer queue, Long success, Long fail) {
        this.name = name;
        this.connect = connect;
        this.totalSpeed = totalSpeed;
        this.loadSpeed = loadSpeed;
        this.total = total;
        this.load = load;
        this.queue = queue;
        this.success = success;
        this.fail = fail;
        this.lastUpdate = getCurrentISOString();
    }    

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    public String getCurrentISOString(){
        Date date = new Date();
        SimpleDateFormat sdf;
        sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(date);
    }


    public String getName() {
        return this.name;
    }

    public Boolean getConnect() {
        return this.connect;
    }

    public Boolean isConnect() {
        return this.connect;
    }

    public Float getTotalSpeed() {
        return this.totalSpeed;
    }

    public Float getLoadSpeed() {
        return this.loadSpeed;
    }

    public Long getTotal() {
        return this.total;
    }

    public Long getLoad() {
        return this.load;
    }

    public Integer getQueue() {
        return this.queue;
    }

    public Long getSuccess() {
        return this.success;
    }

    public Long getFail() {
        return this.fail;
    }

    public String getLastUpdate() {
        return this.lastUpdate;
    }

}
