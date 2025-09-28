var device_noti_offset = 0;
var household_noti_offset = 0;
var house_noti_offset = 0;
var all_spout_prop = {};
var SpoutPropUpdater = setInterval(updateSpoutProp, 10000);
var notificaionInterval = 10000;
var notificationRenderer = setInterval(renderNotification,notificaionInterval);
var logInterval = 5000;
var logRenderer = setInterval(renderLog, logInterval);
var spoutLastUpdate = "";
var notification_list = [];
var log_list = [];
var receive_noti = true;

$(document).ready(()=>{
    function queryDeviceNotification(){
        let xhr = new XMLHttpRequest();
        xhr.onloadend = function(){
            if(this.status == 200){
                let noti_list = JSON.parse(this.responseText);
                device_noti_offset+=noti_list.length;
                noti_list.forEach((ele)=>{
                    switch(ele.type){
                        case -1:
                            $('#device_notification_list').append(`
                                <a href="#" class="list-group-item">
                                    <h4 style="color:#6f6" class="list-group-item-heading">Device under utilized</h4>
                                    <p class="list-group-item-text">
                                    Device ${ele.device_id} of household ${ele.household_id} of house ${ele.house_id} under utilized
                                    <br>
                                    Min: ${ele.min.toFixed(2)} (W)
                                    <br>
                                    Value: <font color="#ff6666">${ele.value.toFixed(2)}</font> (W) (${getChange(ele)})
                                    <br>
                                    Time ${getUniqueID(ele)} (${getTimeAgo(ele)})
                                    </p>
                                </a>
                            `);
                            break;
                        case 0:
                            $('#device_notification_list').append(`
                                <a href="#" class="list-group-item">
                                    <h4 style="color:#BB0" class="list-group-item-heading">Device use more power than usual</h4>
                                    <p class="list-group-item-text">
                                    Device ${ele.device_id} of household ${ele.household_id} of house ${ele.house_id} use more power than usual
                                    <br>
                                    Average: ${ele.avg.toFixed(2)} (W)
                                    <br>
                                    Value: <font color="#ff6666">${ele.value.toFixed(2)}</font> (W) (${getChange(ele)})
                                    <br>
                                    Time ${getUniqueID(ele)} (${getTimeAgo(ele)})
                                    </p>
                                </a>
                            `);
                            break;
                        case 1:
                            $('#device_notification_list').append(`
                                <a href="#" class="list-group-item">
                                    <h4 style="color:#f66" class="list-group-item-heading">Device over utilized</h4>
                                    <p class="list-group-item-text">
                                    Device ${ele.device_id} of household ${ele.household_id} of house ${ele.house_id} over utilized
                                    <br>
                                    Max: ${ele.max.toFixed(2)} (W)
                                    <br>
                                    Value: <font color="#ff6666">${ele.value.toFixed(2)}</font> (W) (${getChange(ele)})
                                    <br>
                                    Time: ${getUniqueID(ele)} (${getTimeAgo(ele)})
                                    </p>
                                </a>
                            `);
                            break;
                    }
                    
                })
            }
        }
        xhr.open("GET", `/api/getdevicenotification?offset=${device_noti_offset}`,true);
        xhr.send();
    }

    function queryHouseholdNotification(){
        let xhr = new XMLHttpRequest();
        xhr.onloadend = function(){
            if(this.status == 200){
                let noti_list = JSON.parse(this.responseText);
                household_noti_offset+=noti_list.length;
                noti_list.forEach((ele)=>{
                    switch(ele.type){
                        case -1:
                            $('#household_notification_list').append(`
                                <a href="#" class="list-group-item">
                                    <h4 style="color:#6f6" class="list-group-item-heading">Household under utilized</h4>
                                    <p class="list-group-item-text">
                                    Household ${ele.household_id} of house ${ele.house_id} under utilized
                                    <br>
                                    Min: ${ele.min.toFixed(2)} (W)
                                    <br>
                                    Value: <font color="#ff6666">${ele.value.toFixed(2)}</font> (W) (${getChange(ele)})
                                    <br>
                                    Time ${getUniqueID(ele)} (${getTimeAgo(ele)})
                                    </p>
                                </a>
                            `);
                            break;
                        case 0:
                            $('#household_notification_list').append(`
                                <a href="#" class="list-group-item">
                                    <h4 style="color:#BB0" class="list-group-item-heading">Household use more power than usual</h4>
                                    <p class="list-group-item-text">
                                    Household ${ele.household_id} of house ${ele.house_id} use more power than usual
                                    <br>
                                    Average: ${ele.avg.toFixed(2)} (W)
                                    <br>
                                    Value: <font color="#ff6666">${ele.value.toFixed(2)}</font> (W) (${getChange(ele)})
                                    <br>
                                    Time ${getUniqueID(ele)} (${getTimeAgo(ele)})
                                    </p>
                                </a>
                            `);
                            break;
                        case 1:
                            $('#household_notification_list').append(`
                                <a href="#" class="list-group-item">
                                    <h4 style="color:#f66" class="list-group-item-heading">Household over utilized</h4>
                                    <p class="list-group-item-text">
                                    Household ${ele.household_id} of house ${ele.house_id} over utilized
                                    <br>
                                    Max: ${ele.max.toFixed(2)} (W)
                                    <br>
                                    Value: <font color="#ff6666">${ele.value.toFixed(2)}</font> (W) (${getChange(ele)})
                                    <br>
                                    Time: ${getUniqueID(ele)} (${getTimeAgo(ele)})
                                    </p>
                                </a>
                            `);
                            break;
                    }
                    
                })
            }
        }
        xhr.open("GET", `/api/gethouseholdnotification?offset=${household_noti_offset}`,true);
        xhr.send();
    }

    function queryHouseNotification(){
        let xhr = new XMLHttpRequest();
        xhr.onloadend = function(){
            if(this.status == 200){
                let noti_list = JSON.parse(this.responseText);
                house_noti_offset+=noti_list.length;
                noti_list.forEach((ele)=>{
                    switch(ele.type){
                        case -1:
                            $('#house_notification_list').append(`
                                <a href="#" class="list-group-item">
                                    <h4 style="color:#6f6" class="list-group-item-heading">House under utilized</h4>
                                    <p class="list-group-item-text">
                                    House ${ele.house_id} under utilized
                                    <br>
                                    Min: ${ele.min.toFixed(2)} (W)
                                    <br>
                                    Value: <font color="#ff6666">${ele.value.toFixed(2)}</font> (W) (${getChange(ele)})
                                    <br>
                                    Time ${getUniqueID(ele)} (${getTimeAgo(ele)})
                                    </p>
                                </a>
                            `);
                            break;
                        case 0:
                            $('#house_notification_list').append(`
                                <a href="#" class="list-group-item">
                                    <h4 style="color:#BB0" class="list-group-item-heading">House use more power than usual</h4>
                                    <p class="list-group-item-text">
                                    House ${ele.house_id} use more power than usual
                                    <br>
                                    Average: ${ele.avg.toFixed(2)} (W)
                                    <br>
                                    Value: <font color="#ff6666">${ele.value.toFixed(2)}</font> (W) (${getChange(ele)})
                                    <br>
                                    Time ${getUniqueID(ele)} (${getTimeAgo(ele)})
                                    </p>
                                </a>
                            `);
                            break;
                        case 1:
                            $('#house_notification_list').append(`
                                <a href="#" class="list-group-item">
                                    <h4 style="color:#f66" class="list-group-item-heading">House over utilized</h4>
                                    <p class="list-group-item-text">
                                    House ${ele.house_id} over utilized
                                    <br>
                                    Max: ${ele.max.toFixed(2)} (W)
                                    <br>
                                    Value: <font color="#ff6666">${ele.value.toFixed(2)}</font> (W) (${getChange(ele)})
                                    <br>
                                    Time: ${getUniqueID(ele)} (${getTimeAgo(ele)})
                                    </p>
                                </a>
                            `);
                            break;
                    }
                    
                })
            }
        }
        xhr.open("GET", `/api/gethousenotification?offset=${house_noti_offset}`,true);
        xhr.send();
    }

    Number.prototype.pad = function(n) {
        return `${String(new Array(n||2).join('0'))}${String(this)}`.slice((n || 2) * -1);
    }

    queryDeviceNotification();
    queryHouseholdNotification();
    queryHouseNotification();

    $('#more_device_notification').on('click', queryDeviceNotification);
    $('#more_household_notification').on('click', queryHouseholdNotification);
    $('#more_house_notification').on('click', queryHouseNotification);

    //Realtime notification
    const socket = io(`${window.location.protocol}//${window.location.host}`, {
        reconnectionDelayMax: 10000
    });

    socket.on('connect', () => {
        console.log(`Connected! #${socket.id}`);
        window.createNotification({
            // close on click
            closeOnClick: false,
                
            // displays close button
            displayCloseButton: true,
            
            // nfc-top-left
            // nfc-bottom-right
            // nfc-bottom-left
            positionClass: 'nfc-bottom-left',
            
            // callback
            onclick: false,
            
            // timeout in milliseconds
            showDuration: 10000,
            
            // success, info, warning, error, and none
            theme: 'success'
        })({
            message: `Connected to notification server!`,
        })
    });

    socket.on('notification', (data)=>{
        if(receive_noti){
            if(notification_list.length>10000) notification_list = [];
            try{
                notification_list.push(JSON.parse(data));
            } catch (e){
                console.log(`Received: ${data}`);
                console.log(e);
            }
        }
    })

    //Storm log
    socket.on('log', (data)=>{
        if(log_list.length>10000) log_list = [];
        log_list.push(data);
    })

    //Spout log
    socket.on('spout', (data)=>{
        try{
            let spout_prop = JSON.parse(data);
            let name = spout_prop.name;
            all_spout_prop[name] = spout_prop;
        } catch (e){
            console.log(`Received: ${data}`);
            console.log(e);
        }
    })

    //on Error
    socket.on('error', (data)=>{
        try{
            let title = "Error"
            let message = data
            window.createNotification({
                // close on click
                closeOnClick: false,
                    
                // displays close button
                displayCloseButton: true,
                
                // nfc-top-left
                // nfc-bottom-right
                // nfc-bottom-left
                positionClass: 'nfc-bottom-right',
                
                // callback
                onclick: false,
                
                // timeout in milliseconds
                showDuration: 10000,
                
                // success, info, warning, error, and none
                theme: 'error'
            })(
            {
                title: title,
                message: message,
            });
        } catch (e){
            console.log(`Received: ${data}`);
            console.log(e);
        }
    })
});

// module
let getUniqueID = (ele) => {
    let index = ele.sliceIndex || ele.slice_index;
    let gap = ele.sliceGap || ele.slice_gap;
    return ele.year + "/" + ele.month + "/" + ele.day + " " +  Math.floor(((index)*(gap))/60).pad() + ":" +  (((index*gap))%60).pad() + "->" + Math.floor((((index)+1)*(gap))/60).pad() + ":" +  ((((index)+1)*(gap))%60).pad() ;
}
let getTimeAgo = (ele) => {
    if(ele){
        let gap = new Date().getTime() - new Date(ele.reg_date||ele.timestamp||ele.lastUpdate).getTime();
        if(gap<2000){
            return "Just now";
        }
        else if(gap<1000*60){
            return Math.floor(gap/1000) + " second(s) ago";
        }
        else if(gap<1000*60*60) {
            return Math.floor(gap/(1000*60)) + " minute(s) ago";
        }
        else if(gap<1000*60*60*24) {
            return Math.floor(gap/(1000*60*60)) + " hour(s) ago";
        }
        else {
            return Math.floor(gap / (1000 * 60 * 60 * 24)) + " day(s) ago";
        }
    }
    else {
        return "Unknow";
    }
}
let getChange = (ele)=>{
    switch(ele.type){
        case -1: 
            return "-" + (ele.value-ele.min).toFixed(2) + "W (" + ((ele.value-ele.min)*100/ele.min).toFixed(2) + "%)";
        case 0:
            return "+" + (ele.value-ele.avg).toFixed(2) + "W (" + ((ele.value-ele.avg)*100/ele.avg).toFixed(2) + "%)";
        case 1: 
            return "+" + (ele.value-ele.max).toFixed(2) + "W (" + ((ele.value-ele.max)*100/ele.max).toFixed(2) + "%)";
    }
}

function updateSpoutProp(){
    let state = "";
    let totalSpeed = 0;
    let loadSpeed = 0;
    let total = 0;
    let load = 0;
    let queue = 0;
    let success = 0;
    let fail = 0;
    let all_connected = true;
    for(let index in all_spout_prop){
        let ele = all_spout_prop[index];
        if(!ele.connect){
            all_connected = false;
            state+=`${ele.name} is disconnected|`;
        }
        totalSpeed+=ele.totalSpeed;
        loadSpeed+=ele.loadSpeed;
        total+=ele.total;
        load+=ele.load;
        queue+=ele.queue;
        success+=ele.success;
        fail+=ele.fail;
    }
    if(all_spout_prop.length==0){
        state = "No data"
    }
    else if(all_connected){
        state = "All spout connected";
    }
    $('#spout-last-update').html(getTimeAgo(Object.values(all_spout_prop)[0]));
    $('#spout-state').html(state);
    $('#spout-receive-speed').html(parseFloat(totalSpeed));
    $('#spout-load-speed').html(parseFloat(loadSpeed));
    $('#spout-receive-total').html(parseInt(total));
    $('#spout-receive-load').html(parseInt(load));
    $('#spout-queue').html(parseInt(queue));
    $('#spout-success').html(parseInt(success));
    $('#spout-fail').html(parseInt(fail));
    all_spout_prop={};
}

function renderNotification(){
    for(let i = 0; i<3; i++){
        try{
            let ele = notification_list.pop();
            if(ele){
                let title = "";
                let message = "";

                switch(ele.type){
                    case -1:
                        title = `Device under utilized`;
                        message= `Device ${ele.deviceId} of household ${ele.householdId} of house ${ele.houseId} under utilized
                                Min: ${ele.min.toFixed(2)} (W)
                                Value: ${ele.value.toFixed(2)} (W) (${getChange(ele)})
                                Time ${getUniqueID(ele)} (${getTimeAgo(ele)})
                                `;
                        break;
                    case 0:
                        title = `Device use more power than usual`;
                        message = `Device ${ele.deviceId} of household ${ele.householdId} of house ${ele.houseId} use more power than usual
                                Average: ${ele.avg.toFixed(2)} (W)
                                Value: ${ele.value.toFixed(2)} (W) (${getChange(ele)})
                                Time ${getUniqueID(ele)} (${getTimeAgo(ele)})
                                `;
                        break;
                    case 1:
                        title = `Device over utilized`
                        message = `Device ${ele.deviceId} of household ${ele.householdId} of house ${ele.houseId} over utilized
                                Max: ${ele.max.toFixed(2)} (W)
                                Value: ${ele.value.toFixed(2)} (W) (${getChange(ele)})
                                Time: ${getUniqueID(ele)} (${getTimeAgo(ele)})`
                        break;
                }
                window.createNotification({
                    // close on click
                    closeOnClick: false,
                        
                    // displays close button
                    displayCloseButton: true,
                    
                    // nfc-top-left
                    // nfc-bottom-right
                    // nfc-bottom-left
                    positionClass: 'nfc-bottom-left',
                    
                    // callback
                    onclick: false,
                    
                    // timeout in milliseconds
                    showDuration: notificaionInterval,
                    
                    // success, info, warning, error, and none
                    theme: 'warning'
                })(
                {
                    title: title,
                    message: message,
                });
            }
        } catch (e){
            console.log(e);
        }
    }
}

function renderLog(){
    for(let i = 0; i<3; i++){
        try{
            let data = log_list.pop();
            if(data){
                let title = "STORM TOPOLOGY LOG"
                let message = data
                window.createNotification({
                    // close on click
                    closeOnClick: false,
                        
                    // displays close button
                    displayCloseButton: true,
                    
                    // nfc-top-left
                    // nfc-bottom-right
                    // nfc-bottom-left
                    positionClass: 'nfc-bottom-right',
                    
                    // callback
                    onclick: false,
                    
                    // timeout in milliseconds
                    showDuration: logInterval,
                    
                    // success, info, warning, error, and none
                    theme: 'info'
                })(
                {
                    title: title,
                    message: message,
                });
            }
        } catch (e){
            console.log(`Received: ${data}`);
            console.log(e);
        }
    }
}