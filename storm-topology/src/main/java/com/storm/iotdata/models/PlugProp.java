package com.storm.iotdata.models;

import java.io.Serializable;

/**
 * Rolling plug statistics used by Bolt_average for anomaly detection.
 */
public class PlugProp implements Serializable {

    private static final long serialVersionUID = 1L;

    public int houseId;
    public int householdId;
    public int plugId;
    public int sliceGap;
    public Double min;
    public Double avg;
    public Double max;
    public Double count;

    public PlugProp(int houseId, int householdId, int plugId, int sliceGap) {
        this.houseId = houseId;
        this.householdId = householdId;
        this.plugId = plugId;
        this.sliceGap = sliceGap;
        this.min = 0.0d;
        this.avg = 0.0d;
        this.max = 0.0d;
        this.count = 0.0d;
    }

    /**
     * Adds a non-zero value to the rolling min/avg/max statistics.
     *
     * @param value New plug average.
     * @return This property object.
     */
    public PlugProp addValue(Double value) {
        if (value == null || value == 0.0d) {
            return this;
        }

        if (count == 0.0d) {
            min = value;
            avg = value;
            max = value;
            count = 1.0d;
            return this;
        }

        avg = (avg * count + value) / (++count);
        min = Math.min(min, value);
        max = Math.max(max, value);
        return this;
    }

    public int getHouseId() {
        return houseId;
    }

    public int getHouseholdId() {
        return householdId;
    }

    public int getPlugId() {
        return plugId;
    }

    public int getSliceGap() {
        return sliceGap;
    }

    public Double getMin() {
        return min;
    }

    public Double getAvg() {
        return avg;
    }

    public Double getMax() {
        return max;
    }

    public Double getCount() {
        return count;
    }

    public String getUniqueId() {
        return String.format("%d-%d-%d-%d", houseId, householdId, plugId, sliceGap);
    }
}
