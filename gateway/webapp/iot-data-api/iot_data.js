class iot_data {
    constructor(house_id, household_id, device_id, year, month, day, slice_gap, slice_index, avg){
        this.house_id = house_id;
        this.household_id = household_id;
        this.device_id = device_id;
        this.year = year;
        this.month = month?parseInt(month).pad():undefined;
        this.day =  day?parseInt(day).pad():undefined;
        this.slice_gap = slice_gap;
        this.slice_index = slice_index;
        this.avg = avg;
    }
    // setAvg = (avg) => {
    //     this.avg = avg;
    // }
    // getAvg = () => {
    //     return this.avg;
    // }
    getUniqueID = () => {
        return this.year + "/" + this.month + "/" + this.day + " " +  Math.floor((this.slice_index*this.slice_gap)/60).pad() + ":" +  ((this.slice_index*this.slice_gap)%60).pad() + "->" + Math.floor(((this.slice_index+1)*this.slice_gap)/60).pad() + ":" +  (((this.slice_index+1)*this.slice_gap)%60).pad() ;
    }
}

class house_data {
    constructor(house_id, year, month, day, slice_gap, slice_index, avg){
        this.house_id = house_id;
        this.year = year;
        this.month = month?parseInt(month).pad():undefined;
        this.day =  day?parseInt(day).pad():undefined;
        this.slice_gap = slice_gap;
        this.slice_index = slice_index;
        this.avg = avg;
    }
    // setAvg = (avg) => {
    //     this.avg = avg;
    // }
    // getAvg = () => {
    //     return this.avg;
    // }
    getUniqueID = () => {
        return this.year + "/" + this.month + "/" + this.day + " " +  Math.floor((this.slice_index*this.slice_gap)/60).pad() + ":" +  ((this.slice_index*this.slice_gap)%60).pad() + "->" + Math.floor(((this.slice_index+1)*this.slice_gap)/60).pad() + ":" +  (((this.slice_index+1)*this.slice_gap)%60).pad() ;
    }
}

class household_data {
    constructor(house_id, household_id, year, month, day, slice_gap, slice_index, avg){
        this.house_id = house_id;
        this.household_id = household_id;
        this.year = year;
        this.month = month?parseInt(month).pad():undefined;
        this.day =  day?parseInt(day).pad():undefined;
        this.slice_gap = slice_gap;
        this.slice_index = slice_index;
        this.avg = avg;
    }
    // setAvg = (avg) => {
    //     this.avg = avg;
    // }
    // getAvg = () => {
    //     return this.avg;
    // }
    getUniqueID = () => {
        return this.year + "/" + this.month + "/" + this.day + " " +  Math.floor((this.slice_index*this.slice_gap)/60).pad() + ":" +  ((this.slice_index*this.slice_gap)%60).pad() + "->" + Math.floor(((this.slice_index+1)*this.slice_gap)/60).pad() + ":" +  (((this.slice_index+1)*this.slice_gap)%60).pad() ;
    }
}

class device_data {
    constructor(house_id, household_id, device_id, year, month, day, slice_gap, slice_index, avg){
        this.house_id = house_id;
        this.household_id = household_id;
        this.device_id = device_id
        this.year = year;
        this.month = month?parseInt(month).pad():undefined;
        this.day =  day?parseInt(day).pad():undefined;
        this.slice_gap = slice_gap;
        this.slice_index = slice_index;
        this.avg = avg;
    }
    // setAvg = (avg) => {
    //     this.avg = avg;
    // }
    // getAvg = () => {
    //     return this.avg;
    // }
    getUniqueID = () => {
        return this.year + "/" + this.month + "/" + this.day + " " +  Math.floor((this.slice_index*this.slice_gap)/60).pad() + ":" +  ((this.slice_index*this.slice_gap)%60).pad() + "->" + Math.floor(((this.slice_index+1)*this.slice_gap)/60).pad() + ":" +  (((this.slice_index+1)*this.slice_gap)%60).pad() ;
    }
}

Number.prototype.pad = function(n) {
    return `${String(new Array(n||2).join('0'))}${String(this)}`.slice((n || 2) * -1);
}

module.exports={
    iot_data, house_data, household_data, device_data
}