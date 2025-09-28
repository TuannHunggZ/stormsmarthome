var myChart;
var interval;
var exportData = {};
// TODO(duongtm3102): revert to ['v0','v1','v2','v3','ml']; later
var version = ['v0'];
var meta_data;
var ctx;

$(document).ready(async function(){
    initController();
    ctx = document.getElementById('IOT').getContext('2d');
    // setTimeout(initController, 60000);
});

async function initGraph(house_id, household_id, device_id, slice_gap, day, version){
    disable();
    if(version.length){
        data = await initData(house_id, household_id, device_id, slice_gap, day);
        forecast_data = await initForecastData(house_id, household_id, device_id, slice_gap, day, version);
        if(day=="*"){
            exportData.name = `house-${house_id}-slice_gap-${slice_gap}-forecast-${version}-full`;
        }
        else{
            exportData.name = `house-${house_id}-slice_gap-${slice_gap}-forecast-${version}-day-${day.replace('/','-')}`;
        }

        let array1 = Object.keys(data);
        let array2 = Object.keys(forecast_data);
        let array3 = array1.concat(array2);
        array3 = [...new Set([...array1,...array2])]

        let sorted_data = [];
        array3.sort().forEach((element,i,arr) => {
            if(data[element]){
                sorted_data.push({
                    x: i,
                    y: data[element].avg
                });
            }
            else {
                sorted_data.push({
                    x: i,
                    y: 0
                });
            }
        });

        let sorted_forecast_data = [];
        array3.sort().forEach((element,i,arr) => {
            if(forecast_data[element]){
                sorted_forecast_data.push({
                    x: i,
                    y: forecast_data[element].avg
                });
            }
            else{
                sorted_forecast_data.push({
                    x: i,
                    y: 0
                });
            }
        });
        myChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: array3.sort(),
                datasets: [{
                    label: `Load of house ${house_id}`,
                    data: sorted_data,
                    backgroundColor: [
                        'rgba(255, 99, 132, 0)'
                    ],
                    borderColor: [
                        'rgba(255, 99, 132, 1)',
                    ],
                    borderWidth: 3
                },{
                    label: `Forecast of house ${house_id}`,
                    data: sorted_forecast_data,
                    backgroundColor: [
                        'rgba(132, 255, 99, 0)'
                    ],
                    borderColor: [
                        'rgba(132, 255, 99, 1)',
                    ],
                    borderWidth: 3
                }]
            }
        });
        enable();
        exportData.data = sorted_data;
        exportData.forecast_data = sorted_forecast_data;
        initForecastMetaData(slice_gap, version);
    }
    else{
        let data = await initData(house_id, household_id, device_id, slice_gap, day);
        let sorted_data = [];
        Object.keys(data).sort().forEach((element,i,arr) => {
            sorted_data.push({
                x: i,
                y: data[element].avg
            });
        });
        myChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: Object.keys(data).sort(),
                datasets: [{
                    label: `Load of house ${house_id}`,
                    data: sorted_data,
                    backgroundColor: [
                        'rgba(255, 99, 132, 0.2)'
                    ],
                    borderColor: [
                        'rgba(255, 99, 132, 1)',
                    ],
                    borderWidth: 3
                }]
            }
        });
        exportData.data = sorted_data;
        calMetadata(data);
        enable();
    }
}

function disable(){
    $('#house').attr('disabled','true');
    $('#household').attr('disabled','true');
    $('#slice_gap').attr('disabled','true');
    $('#day').attr('disabled','true');
    $('#version').attr('disabled','true');
    $('#loader').fadeIn();
}

function enable(){
    $('#house').removeAttr('disabled');
    $('#household').removeAttr('disabled');
    $('#slice_gap').removeAttr('disabled');
    $('#day').removeAttr('disabled');
    $('#version').removeAttr('disabled');
    $('#loader').fadeOut();
}

async function fetchData(){
    if(myChart){
        myChart.destroy();
    }
    await initGraph($('#house').val(),$('#household').val(),$('#device').val(),$('#slice_gap').val(),$('#day').val(),$('#version').val());
}

function Data2CSV(){
    let csvData = "House";
    Object.keys(exportData.data).sort().forEach((key,index,arr)=>{
        csvData+=','+key;
    });
    csvData+="\n";
    csvData+=exportData.data[Object.keys(exportData.data)[0]].house_id;
    Object.keys(exportData.data).sort().forEach((key,index,arr)=>{
        csvData+=','+exportData.data[key].avg;
    });
    csvData+=`\nCount,${exportData.meta_data.count}`;
    csvData+=`\nMean,${exportData.meta_data.mean}`;
    csvData+=`\nVariance,${exportData.meta_data.variance}`;
    csvData+=`\nStandard deviation,${exportData.meta_data.standard_deviation}`;
    if(exportData.meta_data.mse){
        csvData+=`\nMSE,${exportData.meta_data.mse}`;
    }

    blob = new Blob([csvData], { type: 'text/plain' }),
    anchor = document.createElement('a');

    anchor.download = `IOT-Data-${exportData.name}.csv`;
    anchor.href = (window.webkitURL || window.URL).createObjectURL(blob);
    anchor.dataset.downloadurl = ['text/plain', anchor.download, anchor.href].join(':');
    anchor.click();
}

function Data2PNG(){
    var link = document.createElement('a');
    link.download = `IOT-Data-${exportData.name}.png`;
    link.href = document.getElementById('IOT').toDataURL();
    link.click();
}

async function initMeta(){
    return new Promise((resolve, reject)=>{
        let xhr = new XMLHttpRequest();
        xhr.onloadend = function(){
            if(this.status == 200){
                meta_data = JSON.parse(this.responseText);
                resolve(JSON.parse(this.responseText));
            }
            else{
                reject(this.status);
            }
        };
        xhr.open("GET", "api/getmeta");
        xhr.send();
    })
}

async function initController(){
    meta_data = await initMeta();
    let house_id_list = Object.keys(meta_data.house_data_topo);
    let slice_gap = meta_data.slice_gap;

    $(".nav-tabs a").click(function(){
        $(this).tab('show');
        query_type = ($(this).attr("href"));
        fetchData();
    });
    $('#house').empty();
    house_id_list.forEach(i=>{
        $('#house').append(`
            <option value="${i}">House ${i}</option>
        `);
    })
    let household_list = Object.keys(meta_data.household_data_topo[$('#house').val()]);
    $('#device').empty();
    $('#device').attr('disabled', 'disabled');
    $('#household').empty();
    $('#household').append(`
        <option value="*">All household</option>
    `)
    if(household_list){
        household_list.forEach(ele=>{
            $('#household').append(`
                <option value="${ele}">Household ${ele}</option>
            `);
        });
    }
    else {
        setTimeout(initController, 10000);
    }
    $('#household').removeAttr('disabled');
    $('#slice_gap').empty();
    slice_gap.forEach(ele=>{
        $('#slice_gap').append(`
            <option value="${ele}">${ele} minutes</option>
        `);
    })
    $('#version').empty();
    $('#version').append(`
        <option value="">No forecast</option>
    `);
    version.forEach(ele=>{
        $('#version').append(`
            <option value="${ele}">${ele}</option>
        `);
    })
    let day_list;
    if($('#household').val() === "*"){
        day_list = meta_data.house_data_topo[$('#house').val()];
    }
    else if ($('#device').val() === "*") {
        day_list = meta_data.household_data_topo[$('#house').val()][$('#household').val()];
    }
    else{
        day_list = meta_data.device_data_topo[$('#house').val()][$('#household').val()][$('#household').val()];
    }
    $('#day').empty();
    $('#day').append(`
            <option value="*">All</option>
        `)
    if(day_list){
        day_list.forEach(ele=>{
            $('#day').append(`
                <option value="${ele.text}">${ele.text}</option>
            `);
        });
    }
    else {
        setTimeout(initController, 10000);
    }

    fetchData();
    if(interval){
        clearInterval(interval);
    }
    interval = setInterval(fetchData, 60000);

    $('#house').on('change',function(){
        let household_list = Object.keys(meta_data.household_data_topo[$('#house').val()]);
        $('#device').empty();
        $('#device').attr('disabled', 'disabled');
        $('#household').empty();
        $('#household').append(`
            <option value="*">All household</option>
        `)
        if(household_list){
            household_list.forEach(ele=>{
                $('#household').append(`
                    <option value="${ele}">Household ${ele}</option>
                `);
            });
        }
        else {
            setTimeout(initController, 10000);
        }
        $('#household').removeAttr('disabled');
        let day_list = meta_data.house_data_topo[$('#house').val()];
        $('#day').empty();
        $('#day').append(`
            <option value="*">All</option>
        `)
        day_list.forEach(ele=>{
            $('#day').append(`
                <option value="${ele.text}">${ele.text}</option>
            `);
        });
        fetchData();
    });

    $('#household').on('change',function(){
        if($('#household').val() != "*"){
            let device_list = Object.keys(meta_data.device_data_topo[$('#house').val()][$('#household').val()]);
            $('#device').empty();
            $('#device').append(`
                <option value="*">All device</option>
            `)
            if(device_list){
                device_list.forEach(ele=>{
                    $('#device').append(`
                        <option value="${ele}">Device ${ele}</option>
                    `);
                });
            }
            else {
                setTimeout(initController, 10000);
            }
            $('#device').removeAttr('disabled');
            let day_list = meta_data.household_data_topo[$('#house').val()][$('#household').val()];
            $('#day').empty();
            $('#day').append(`
                <option value="*">All</option>
            `)
            day_list.forEach(ele=>{
                $('#day').append(`
                    <option value="${ele.text}">${ele.text}</option>
                `);
            });
            fetchData();
        }
        else {
            $('#device').empty();
            $('#device').attr('disabled', 'disabled');
            let day_list = meta_data.house_data_topo[$('#house').val()];
            $('#day').empty();
            $('#day').append(`
                <option value="*">All</option>
            `)
            day_list.forEach(ele=>{
                $('#day').append(`
                    <option value="${ele.text}">${ele.text}</option>
                `);
            });
            fetchData();
        }
    });

    $('#device').on('change',function(){
        if($('#device').val()!="*"){
            let day_list = meta_data.device_data_topo[$('#house').val()][$('#household').val()][$('#device').val()];
            $('#day').empty();
            $('#day').append(`
                <option value="*">All</option>
            `)
            day_list.forEach(ele=>{
                $('#day').append(`
                    <option value="${ele.text}">${ele.text}</option>
                `);
            });
            fetchData();
        }
        else {
            let day_list = meta_data.household_data_topo[$('#house').val()][$('#household').val()];
            $('#day').empty();
            $('#day').append(`
                <option value="*">All</option>
            `)
            day_list.forEach(ele=>{
                $('#day').append(`
                    <option value="${ele.text}">${ele.text}</option>
                `);
            });
            fetchData();
        }
    });

    $('#slice_gap').on('change',function(){
        fetchData();
    });
    $('#day').on('change',function(){
        fetchData();
    })
    $('#version').on('change',function(){
        fetchData();
    })
}

async function initData(house_id, household_id, device_id, slice_gap, day){
    return new Promise((resolve,reject)=>{
        var xhr = new XMLHttpRequest();
        xhr.onloadend = function(){
            enable();
            if(this.status == 200){
                if(this.responseText){
                    let data = JSON.parse(this.responseText);
                    exportData.data = data;
                    if(day=="*"){
                        exportData.name = `house-${house_id}-slice_gap-${slice_gap}-full`;
                    }
                    else{
                        exportData.name = `house-${house_id}-slice_gap-${slice_gap}-day-${day}`;
                    }
                    resolve(data);
                }
                else{
                    reject({});
                    window.alert("No data");
                }
            }
            else{
                reject(this.status);
                window.alert("Cannot fetch data from API");
            }
        }
        if(day=="*") xhr.open("GET",`/api/query?house_id=${house_id}&household_id=${household_id}&device_id=${device_id}&slice_gap=${slice_gap}`);
        else xhr.open("GET",`/api/query?house_id=${house_id}&household_id=${household_id}&device_id=${device_id}&slice_gap=${slice_gap}&year=${day.split('/')[0]}&month=${day.split('/')[1]}&day=${day.split('/')[2]}`);
        xhr.send();
    });
}

async function initForecastData(house_id, household_id, device_id, slice_gap, day, version){
    return new Promise((resolve, reject)=>{
        var xhr2 = new XMLHttpRequest();
        xhr2.onloadend = function(){
            if(this.status == 200){
                if(this.responseText){
                    let forecast_data = JSON.parse(this.responseText);
                    resolve(forecast_data);
                }
                else{
                    reject({});
                }
            }
            else {
                reject(this.status);
            }
        }
        if(day=="*") xhr2.open("GET",`/api/queryforecast?house_id=${house_id}&household_id=${household_id}&device_id=${device_id}&slice_gap=${slice_gap}&version=${version}`);
        else xhr2.open("GET",`/api/queryforecast?house_id=${house_id}&household_id=${household_id}&device_id=${device_id}&slice_gap=${slice_gap}&version=${version}&year=${day.split('/')[0]}&month=${day.split('/')[1]}&day=${day.split('/')[2]}`);
        xhr2.send();
    });
}

function initForecastMetaData(slice_gap, version){
    let xhr3 = new XMLHttpRequest();
    xhr3.onloadend = function(){
        if(this.status==200){
            let meta_data = JSON.parse(this.responseText);
            $('#datapoint').html(meta_data.count);
            $('#mean').html(meta_data.mean.toFixed(3));
            $('#variance').html(meta_data.variance.toFixed(3));
            $('#sd').html((meta_data.standard_deviation).toFixed(3));
            $('#mse').html((meta_data.mse).toFixed(3));
            exportData.meta_data = {
                count: meta_data.count,
                mean: meta_data.mean,
                variance: meta_data.variance,
                standard_deviation: meta_data.standard_deviation,
                mse: meta_data.mse
            }
        }
        else{
            window.alert("Cannot get metadata");
        }
    }
    xhr3.open("GET", `/api/getforecastmetadata?slice_gap=${slice_gap}&version=${version}`);
    xhr3.send();
}

function calMetadata(data) {
    //Calculating mean
    let total = 0;
    let count = 0;
    Object.keys(data).forEach(ele=>{
        total+=data[ele].avg;
        count++;
    });
    let mean = total/count;

    //Calculating variance and standard deviation
    let delta_total = 0;
    let delta_count = 0;
    Object.keys(data).forEach(ele=>{
        delta_total += (data[ele].avg-mean)**2;
        delta_count++;
    });
    let sd = Math.sqrt((delta_total/delta_count));
    $('#datapoint').html(Object.keys(data).length);
    $('#mean').html(mean.toFixed(3));
    $('#variance').html((delta_total/delta_count).toFixed(3));
    $('#sd').html(Math.sqrt((delta_total/delta_count)).toFixed(3));
    $('#mse').html("Not available");
    exportData.meta_data = {
        count: Object.keys(data).length,
        mean: mean.toFixed(3),
        variance: (delta_total/delta_count).toFixed(3),
        standard_deviation: Math.sqrt((delta_total/delta_count)).toFixed(3),
    }
}