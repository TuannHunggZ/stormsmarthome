const mysql = require('mysql');
let cred = require('./cred.json');
const house_data = require('./iot_data').house_data;
const household_data = require('./iot_data').household_data;
const device_data = require('./iot_data').device_data;
const iot_data = require('./iot_data').iot_data;

var con;
var meta_data = {
    err: null,
    result: {}
}

initCon();

function initCon(){
    try{
        if(con){
            con.end(function(err) {
                if (err) {
                    console.log('error:' + err.message);
                    con.destroy();
                    console.log('Destroy the database connection.');
                }
                else {
                    console.log('Close the database connection.');
                }
            });
        }
    } catch (e){
        console.log(e);
    }
    con = mysql.createConnection({
        host: cred.db_url,
        port: cred.db_port||3306,
        user: cred.db_user,
        password: cred.db_pass,
        database: cred.db_name
    });
    
    con.connect(async function(err) {
        if (err) {
            console.log(err);
            setTimeout(()=>{
                console.log("Retrying");
                initCon();
            },5000)
        }
        else{
            console.log('DB Connected');
            await initMeta();
            setInterval(initMeta, 60000);
            // con.query('drop table forecast_meta_data', ()=>{});
            // let sql = `create table forecast_meta_data(version VARCHAR(4) NOT NULL, slice_gap INT NOT NULL, count INT UNSIGNED DEFAULT 0, mean DOUBLE UNSIGNED DEFAULT 0, variance DOUBLE UNSIGNED DEFAULT 0, standard_deviation DOUBLE UNSIGNED DEFAULT 0, mse DOUBLE UNSIGNED DEFAULT 0, reg_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, PRIMARY KEY(slice_gap, version))`;
            // con.query(sql,function (err, result, fields) {
            //     if (err) {
            //         console.log(err);
            //     }
            //     else{
            //         console.log("Created table");
            //     }
            // })
            recalculateMSE();
            setInterval(recalculateMSE,60*60*1000);
        }
    });
}
try{
    function query(iot_data, callback) {
        if(iot_data.household_id==="*" || !iot_data.household_id){
            let sql = `SELECT * FROM house_data WHERE `;
            if(iot_data.house_id!=undefined && iot_data.house_id!=null) sql+=`house_id=${iot_data.house_id} AND `;
            if(iot_data.year!=undefined && iot_data.year!=null) sql+=`year=${iot_data.year} AND `;
            if(iot_data.month!=undefined && iot_data.month!=null) sql+=`month=${iot_data.month} AND `;
            if(iot_data.day!=undefined && iot_data.day!=null) sql+=`day=${iot_data.day} AND `;
            if(iot_data.slice_gap!=undefined && iot_data.slice_gap!=null) sql+=`slice_gap=${iot_data.slice_gap} AND `;
            if(iot_data.slice_index!=undefined && iot_data.slice_index!=null) sql+=`slice_index=${iot_data.slice_index} AND `;
            con.query(sql.substring(0,sql.length-4), function (err, result, fields) {
                if (err) {
                    console.log(err);
                    setTimeout(()=>{
                        console.log("Retrying");
                        initCon();
                    },5000)
                }
                else if(result){
                    var house_datas = {};
                    result.forEach((ele,i,arr) => {
                        let temp = new house_data(ele.house_id, ele.year, ele.month, ele.day, ele.slice_gap, ele.slice_index, ele.avg);
                        house_datas[temp.getUniqueID()] = temp;
                    });
                    callback(err, house_datas);
                }
                else{
                    callback(err, {});
                }
            });
        }
        else if(iot_data.device_id==="*" || !iot_data.device_id){
            let sql = `SELECT * FROM household_data WHERE `;
            if(iot_data.house_id!=undefined && iot_data.house_id!=null) sql+=`house_id=${iot_data.house_id} AND `;
            if(iot_data.household_id!=undefined && iot_data.household_id!=null) sql+=`household_id=${iot_data.household_id} AND `;
            if(iot_data.year!=undefined && iot_data.year!=null) sql+=`year=${iot_data.year} AND `;
            if(iot_data.month!=undefined && iot_data.month!=null) sql+=`month=${iot_data.month} AND `;
            if(iot_data.day!=undefined && iot_data.day!=null) sql+=`day=${iot_data.day} AND `;
            if(iot_data.slice_gap!=undefined && iot_data.slice_gap!=null) sql+=`slice_gap=${iot_data.slice_gap} AND `;
            if(iot_data.slice_index!=undefined && iot_data.slice_index!=null) sql+=`slice_index=${iot_data.slice_index} AND `;
            con.query(sql.substring(0,sql.length-4), function (err, result, fields) {
                if (err) {
                    console.log(err);
                    setTimeout(()=>{
                        console.log("Retrying");
                        initCon();
                    },5000)
                }
                else if(result){
                    var house_datas = {};
                    result.forEach((ele,i,arr) => {
                        let temp = new house_data(ele.house_id, ele.year, ele.month, ele.day, ele.slice_gap, ele.slice_index, ele.avg);
                        house_datas[temp.getUniqueID()] = temp;
                    });
                    callback(err, house_datas);
                }
                else{
                    callback(err, {});
                }
            });
        }
        else {
            let sql = `SELECT * FROM device_data WHERE `;
            if(iot_data.house_id!=undefined && iot_data.house_id!=null) sql+=`house_id=${iot_data.house_id} AND `;
            if(iot_data.household_id!=undefined && iot_data.household_id!=null) sql+=`household_id=${iot_data.household_id} AND `;
            if(iot_data.device_id!=undefined && iot_data.device_id!=null) sql+=`device_id=${iot_data.device_id} AND `;
            if(iot_data.year!=undefined && iot_data.year!=null) sql+=`year=${iot_data.year} AND `;
            if(iot_data.month!=undefined && iot_data.month!=null) sql+=`month=${iot_data.month} AND `;
            if(iot_data.day!=undefined && iot_data.day!=null) sql+=`day=${iot_data.day} AND `;
            if(iot_data.slice_gap!=undefined && iot_data.slice_gap!=null) sql+=`slice_gap=${iot_data.slice_gap} AND `;
            if(iot_data.slice_index!=undefined && iot_data.slice_index!=null) sql+=`slice_index=${iot_data.slice_index} AND `;
            con.query(sql.substring(0,sql.length-4), function (err, result, fields) {
                if (err) {
                    console.log(err);
                    setTimeout(()=>{
                        console.log("Retrying");
                        initCon();
                    },5000)
                }
                else if(result){
                    var house_datas = {};
                    result.forEach((ele,i,arr) => {
                        let temp = new house_data(ele.house_id, ele.year, ele.month, ele.day, ele.slice_gap, ele.slice_index, ele.avg);
                        house_datas[temp.getUniqueID()] = temp;
                    });
                    callback(err, house_datas);
                }
                else{
                    callback(err, {});
                }
            });
        }
    }

    function querybyweek(iot_data, week, callback) {
        let final_result = {};
        let done = false;
        for(var i = 0; i<7; i++){
            query(new house_data(iot_data.house_id,iot_data.year, iot_data.month, 1+week*7+i, iot_data.slice_gap, iot_data.slice_index), (err,result)=>{
                if (err) {
                    callback(err,null);
                }
                jsonConcat(final_result,result);
                if(!result[Object.keys(result)[0]] || parseInt(result[Object.keys(result)[0]].day-1)%7==6){
                    if(!done)
                        callback(null,final_result);
                    done = true;
                }
            });
        }
    }
    
    function queryforecast(iot_data, version, callback) {
        if(iot_data.household_id==="*" || !iot_data.household_id){
            let sql = `SELECT * FROM house_data_forecast_${version} WHERE `;
            if(iot_data.house_id!=undefined && iot_data.house_id!=null) sql+=`house_id=${iot_data.house_id} AND `;
            if(iot_data.year!=undefined && iot_data.year!=null) sql+=`year=${iot_data.year} AND `;
            if(iot_data.month!=undefined && iot_data.month!=null) sql+=`month=${iot_data.month} AND `;
            if(iot_data.day!=undefined && iot_data.day!=null) sql+=`day=${iot_data.day} AND `;
            if(iot_data.slice_gap!=undefined && iot_data.slice_gap!=null) sql+=`slice_gap=${iot_data.slice_gap} AND `;
            if(iot_data.slice_index!=undefined && iot_data.slice_index!=null) sql+=`slice_index=${iot_data.slice_index} AND `;
            con.query(sql.substring(0,sql.length-4), function (err, result, fields) {
                if (err) {
                    console.log(err);
                    setTimeout(()=>{
                        console.log("Retrying");
                        initCon();
                    },5000)
                }
                if(result){
                    var house_datas = {};
                    result.forEach((ele,i,arr) => {
                        let temp = new house_data(ele.house_id, ele.year, ele.month, ele.day, ele.slice_gap, ele.slice_index, ele.avg);
                        house_datas[temp.getUniqueID()] = temp;
                    });
                    callback(err, house_datas);
                }
                else {
                    callback(err, null);
                }
            });
        }
        else if(iot_data.device_id==="*" || !iot_data.device_id){
            let sql = `SELECT * FROM household_data_forecast_${version} WHERE `;
            if(iot_data.house_id!=undefined && iot_data.house_id!=null) sql+=`house_id=${iot_data.house_id} AND `;
            if(iot_data.household_id!=undefined && iot_data.household_id!=null) sql+=`household_id=${iot_data.household_id} AND `;
            if(iot_data.year!=undefined && iot_data.year!=null) sql+=`year=${iot_data.year} AND `;
            if(iot_data.month!=undefined && iot_data.month!=null) sql+=`month=${iot_data.month} AND `;
            if(iot_data.day!=undefined && iot_data.day!=null) sql+=`day=${iot_data.day} AND `;
            if(iot_data.slice_gap!=undefined && iot_data.slice_gap!=null) sql+=`slice_gap=${iot_data.slice_gap} AND `;
            if(iot_data.slice_index!=undefined && iot_data.slice_index!=null) sql+=`slice_index=${iot_data.slice_index} AND `;
            con.query(sql.substring(0,sql.length-4), function (err, result, fields) {
                if (err) {
                    console.log(err);
                    setTimeout(()=>{
                        console.log("Retrying");
                        initCon();
                    },5000)
                }
                if(result){
                    var household_datas = {};
                    result.forEach((ele,i,arr) => {
                        let temp = new household_data(ele.house_id, ele.household_id, ele.year, ele.month, ele.day, ele.slice_gap, ele.slice_index, ele.avg);
                        household_datas[temp.getUniqueID()] = temp;
                    });
                    callback(err, household_datas);
                }
                else {
                    callback(err, null);
                }
            });
        }
        else{
            let sql = `SELECT * FROM device_data_forecast_${version} WHERE `;
            if(iot_data.house_id!=undefined && iot_data.house_id!=null) sql+=`house_id=${iot_data.house_id} AND `;
            if(iot_data.household_id!=undefined && iot_data.household_id!=null) sql+=`household_id=${iot_data.household_id} AND `;
            if(iot_data.device_id!=undefined && iot_data.device_id!=null) sql+=`device_id=${iot_data.device_id} AND `;
            if(iot_data.year!=undefined && iot_data.year!=null) sql+=`year=${iot_data.year} AND `;
            if(iot_data.month!=undefined && iot_data.month!=null) sql+=`month=${iot_data.month} AND `;
            if(iot_data.day!=undefined && iot_data.day!=null) sql+=`day=${iot_data.day} AND `;
            if(iot_data.slice_gap!=undefined && iot_data.slice_gap!=null) sql+=`slice_gap=${iot_data.slice_gap} AND `;
            if(iot_data.slice_index!=undefined && iot_data.slice_index!=null) sql+=`slice_index=${iot_data.slice_index} AND `;
            con.query(sql.substring(0,sql.length-4), function (err, result, fields) {
                if (err) {
                    console.log(err);
                    setTimeout(()=>{
                        console.log("Retrying");
                        initCon();
                    },5000)
                }
                else if(result){
                    var device_datas = {};
                    result.forEach((ele,i,arr) => {
                        let temp = new device_data(ele.house_id, ele.household_id, ele.device_id, ele.year, ele.month, ele.day, ele.slice_gap, ele.slice_index, ele.avg);
                        device_datas[temp.getUniqueID()] = temp;
                    });
                    callback(err, device_datas);
                }
                else{
                    callback(err, {});
                }
            });
        }
    }

    function queryforecastbyweek(iot_data, week , version, callback) {
        let final_result = {};
        let done = false;
        for(var i = 0; i<7; i++){
            queryforecast(new house_data(iot_data.house_id,iot_data.year, iot_data.month, 1+week*7+i, iot_data.slice_gap, iot_data.slice_index), version, (err,result)=>{
                if (err) {
                    callback(err,null);
                }
                jsonConcat(final_result,result);
                if(!result[Object.keys(result)[0]] || parseInt(result[Object.keys(result)[0]].day-1)%7==6){
                    if(!done)
                        callback(null,final_result);
                    done = true;
                }
            });
        }
    }

    function getforecastmetadata(version, slice_gap, callback) {
        if(version && slice_gap){
            let sql = `select * from forecast_meta_data where version="${version}" AND slice_gap=${slice_gap}`;
            con.query(sql, (err,result)=>{
                if(err) {
                    callback(err,null);
                }
                else{
                    callback(err,result);
                }
            });
        }
        else{
            callback(400, null);
        }
    }

    function getdevicenotifications(house_id, household_id, device_id, offset, limit, callback) {
        if(!isNaN(house_id) && !isNaN(household_id) && !isNaN(device_id)){
            let sql = `select * from device_notification where house_id="${house_id}" AND household_id=${household_id} AND device_id=${device_id} order by reg_date desc limit ${offset}, ${limit}`;
            con.query(sql, (err,result)=>{
                if(err) {
                    callback(err,null);
                }
                else{
                    callback(err,result);
                }
            });
        }
        else if(!isNaN(house_id) && !isNaN(household_id)){
            let sql = `select * from device_notification where house_id="${house_id}" AND household_id=${household_id} order by reg_date desc limit ${offset}, ${limit}`;
            con.query(sql, (err,result)=>{
                if(err) {
                    callback(err,null);
                }
                else{
                    callback(err,result);
                }
            });
        }
        else if(!isNaN(house_id)){
            let sql = `select * from device_notification where house_id="${house_id}" order by reg_date desc limit ${offset}, ${limit}`;
            con.query(sql, (err,result)=>{
                if(err) {
                    callback(err,null);
                }
                else{
                    callback(err,result);
                }
            });
        }
        else{
            let sql = `select * from device_notification order by reg_date desc limit ${offset}, ${limit}`;
            con.query(sql, (err,result)=>{
                if(err) {
                    callback(err,null);
                }
                else{
                    callback(err,result);
                }
            });
        }
    }

    function gethouseholdnotifications(house_id, household_id, offset, limit, callback) {
        if(!isNaN(house_id) && !isNaN(household_id)){
            let sql = `select * from household_notification where house_id="${house_id}" AND household_id=${household_id} order by reg_date desc limit ${offset}, ${limit}`;
            con.query(sql, (err,result)=>{
                if(err) {
                    callback(err,null);
                }
                else{
                    callback(err,result);
                }
            });
        }
        else if(!isNaN(house_id)){
            let sql = `select * from household_notification where house_id="${house_id}" order by reg_date desc limit ${offset}, ${limit}`;
            con.query(sql, (err,result)=>{
                if(err) {
                    callback(err,null);
                }
                else{
                    callback(err,result);
                }
            });
        }
        else{
            let sql = `select * from household_notification order by reg_date desc limit ${offset}, ${limit}`;
            con.query(sql, (err,result)=>{
                if(err) {
                    callback(err,null);
                }
                else{
                    callback(err,result);
                }
            });
        }
    }

    function gethousenotifications(house_id, offset, limit, callback) {
        if(!isNaN(house_id)){
            let sql = `select * from house_notification where house_id="${house_id}" order by reg_date desc limit ${offset}, ${limit}`;
            con.query(sql, (err,result)=>{
                if(err) {
                    callback(err,null);
                }
                else{
                    callback(err,result);
                }
            });
        }
        else{
            let sql = `select * from house_notification order by reg_date desc limit ${offset}, ${limit}`;
            con.query(sql, (err,result)=>{
                if(err) {
                    callback(err,null);
                }
                else{
                    callback(err,result);
                }
            });
        }
    }

    function getMeta(callback){
        callback(meta_data.err, meta_data.result);
    }
    
    module.exports = {
        query, queryforecast, querybyweek, queryforecastbyweek, getforecastmetadata, getdevicenotifications, gethouseholdnotifications, gethousenotifications, getMeta
    }
}
catch(err){
    if (err) {
        console.log(err);
        setTimeout(()=>{
            console.log("Retrying");
            initCon();
        },5000)
    }
}

function jsonConcat(o1, o2) {
    for (var key in o2) {
     o1[key] = o2[key];
    }
    return o1;
}

function recalculateMSE(){
    try{
        let slice_gap = [1,5,10,15,20,30,60,120];
        slice_gap.forEach(window=>{
            let data = [];
            query(new house_data(null,null,null,null,window,null),(err,result)=>{
                Object.keys(result).forEach(key=>{
                    data.push(result[key]);
                })
                // TODO(duongtm3102): revert to ['v0','v1','v2','v3','ml']; later
                let versions = ['v0'];
                versions.forEach(ver=>{
                    let forecast_data = [];
                    queryforecast(new house_data(null,null,null,null,window,null),ver,(err,forecast_result)=>{
                        if(err){
                            console.log(err);
                        }
                        else{
                            Object.keys(forecast_result).forEach(key=>{
                                forecast_data.push(forecast_result[key]);
                            })
                            let temp = data.concat(forecast_data);
                            let total = 0;
                            temp.forEach(sample=>{
                                total+=sample.avg;
                            });
                            let mean = total/temp.length;
                            let delta_total = 0;
                            temp.forEach(sample=>{
                                delta_total+=(sample.avg-mean)**2;
                            });
                            let variance = delta_total/(temp.length-1);
                            let standard_deviation = Math.sqrt(variance);
                            let mse_total = 0;
                            let mse_count = 0;
                            Object.keys(result).forEach(ele=>{
                                if(forecast_result[ele]){
                                    mse_total+= (((result[ele].avg-mean)/standard_deviation)-((forecast_result[ele].avg)-mean)/standard_deviation)**2;
                                    mse_count++;
                                }
                            });
                            let mse = mse_total/mse_count;
                            let sql = `insert into forecast_meta_data(version,slice_gap,count,mean,variance,standard_deviation,mse) values ("${ver}",${window},${forecast_data.length},${mean},${variance},${standard_deviation},${mse}) on duplicate key update count=VALUES(count), mean=VALUES(mean), variance=VALUES(variance), standard_deviation=VALUES(standard_deviation), mse=VALUES(mse)`;
                            con.query(sql, (err,result)=>{
                                if(err){
                                    console.log(sql);
                                }
                                else{
                                    console.log(`[Forecast ${ver} slice_gap ${window}] Count: ${temp.length}\tMean: ${mean.toFixed(3)}\tVariance: ${variance.toFixed(3)}\tStandard deviation: ${standard_deviation.toFixed(3)}\tMSE: ${mse}`);
                                }
                            })
                        }
                    })
                })
            });
        })
    } catch (err) {
        console.log(err);
    }
}

async function initMeta(){
    return new Promise((resolve,reject)=>{
        let con_alt = mysql.createConnection({
            host: cred.db_url,
            port: cred.db_port||3306,
            user: cred.db_user,
            password: cred.db_pass,
            database: cred.db_name
        });
        
        con_alt.connect(async function(err) {
            if (!err) {
                let sql = `select distinct slice_gap from house_data order by slice_gap asc`;
                con_alt.query(sql, (err,result)=>{
                    if(err || result == null) {
                        console.log(err);
                        meta_data.err = err;
                        con_alt.end(function(err) {
                            if (err) {
                                console.log('error:' + err.message);
                                con_alt.destroy();
                            }
                        });
                        reject();
                    }
                    else{
                        meta_data.result.slice_gap = [];
                        result.forEach(ele=>{
                            meta_data.result.slice_gap.push(ele.slice_gap);
                        })
                        let sql = `select distinct house_id, year, month, day, concat(year, '/', month, '/', day) as date_text from house_data order by date_text asc`;
                        con_alt.query(sql, (err,result)=>{
                            if(err || result == null) {
                                console.log(err);
                                meta_data.err = err;
                                con_alt.end(function(err) {
                                    if (err) {
                                        console.log('error:' + err.message);
                                        con_alt.destroy();
                                    }
                                });
                                reject();
                            }
                            else{
                                meta_data.err = null;
                                meta_data.result.house_data_topo = {};
                                result.forEach(ele=>{
                                    if(meta_data.result.house_data_topo[ele.house_id] == undefined){
                                        meta_data.result.house_data_topo[ele.house_id] = [];
                                    }
                                    meta_data.result.house_data_topo[ele.house_id].push({
                                        year: ele.year,
                                        month: ele.month,
                                        day: ele.day,
                                        text: ele.date_text
                                    })
                                })
                                let sql = `select distinct house_id, household_id, year, month, day, concat(year, '/', month, '/', day) as date_text from household_data order by date_text asc`;
                                con_alt.query(sql, (err,result)=>{
                                    if(err || result == null) {
                                        console.log(err);
                                        meta_data.err = err;
                                        con_alt.end(function(err) {
                                            if (err) {
                                                console.log('error:' + err.message);
                                                con_alt.destroy();
                                            }
                                        });
                                        reject();
                                    }
                                    else{
                                        meta_data.result.household_data_topo = {};
                                        result.forEach(ele=>{
                                            if(meta_data.result.household_data_topo[ele.house_id] == undefined){
                                                meta_data.result.household_data_topo[ele.house_id] = {};
                                            }
                                            if(meta_data.result.household_data_topo[ele.house_id][ele.household_id] == undefined){
                                                meta_data.result.household_data_topo[ele.house_id][ele.household_id] = [];
                                            }
                                            meta_data.result.household_data_topo[ele.house_id][ele.household_id].push({
                                                year: ele.year,
                                                month: ele.month,
                                                day: ele.day,
                                                text: ele.date_text
                                            })
                                        })
                                        let sql = `select distinct house_id, household_id, device_id, year, month, day, concat(year, '/', month, '/', day) as date_text from device_data order by date_text asc`;
                                        con_alt.query(sql, (err,result)=>{
                                            if(err || result == null) {
                                                console.log(err);
                                                meta_data.err = err;
                                                con_alt.end(function(err) {
                                                    if (err) {
                                                        console.log('error:' + err.message);
                                                        con_alt.destroy();
                                                    }
                                                });
                                                reject();
                                            }
                                            else{
                                                meta_data.result.device_data_topo = {};
                                                result.forEach(ele=>{
                                                    if(meta_data.result.device_data_topo[ele.house_id] == undefined){
                                                        meta_data.result.device_data_topo[ele.house_id] = {};
                                                    }
                                                    if(meta_data.result.device_data_topo[ele.house_id][ele.household_id] == undefined){
                                                        meta_data.result.device_data_topo[ele.house_id][ele.household_id] = {};
                                                    }
                                                    if(meta_data.result.device_data_topo[ele.house_id][ele.household_id][ele.device_id] == undefined) {
                                                        meta_data.result.device_data_topo[ele.house_id][ele.household_id][ele.device_id] = [];
                                                    }
                                                    meta_data.result.device_data_topo[ele.house_id][ele.household_id][ele.device_id].push({
                                                        year: ele.year,
                                                        month: ele.month,
                                                        day: ele.day,
                                                        text: ele.date_text
                                                    })
                                                })
                                                con_alt.end(function(err) {
                                                    if (err) {
                                                        console.log('error:' + err.message);
                                                        con_alt.destroy();
                                                    }
                                                });
                                                resolve();
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            }
        });
    });
}
