## MQTT publisher tutorial
Read IoT data from a CSV file and send it to a broker as an MQTT client. Many thanks to [@HiIamLala](https://github.com/HiIamLala)

**Requirement**

First, install the sample file by downloading it from this [link](https://drive.google.com/file/d/14nO_NhyyJ_ig25RqvTS4wrkm-Zv1revR/view) and down by gdown from pip library

Then exact samplefile.zip and copy sample file to folder `data-file` like image below
![alt text](../../image/image.png)

```cmd
cd mqtt

mkdir data-file

python3 -m venv venv

source venv/bin/activate

pip install gdown

gdown 14nO_NhyyJ_ig25RqvTS4wrkm-Zv1revR

tar -xzf debs40houses16h.tar.gz -C data-file/
```

**Installation-Run program**

Go to root path which is mqtt-publisher

```cmd
cd /mqtt/mqtt-publisher/
```

Require NodeJS minimum version 12, npm.

Install dependencies with npm
```bash
npm install
```

Start application
```bash
node index.js [options value]
```

options: 
-     -h        set High Water Mark of input stream (default 200)
-     -f        set path to input file (REQUIRE)
-     -c        set first constrainer value, sleep between each mqtt message (ms) (default 10)
                ! this value will change to fit the transmission speed.
-     -s        set expected speed. (messages per second)
-     -b        set Broker URL (default tcp://mqtt-broker)
-     -t        set topic to publish message

Example
```cmd
node index.js -f ../data-file/house-0.csv -s 100 -b 127.0.0.1
```

**Run with docker**

Build image

```cmd
docker build -t mqtt-publisher .
```

Run image

```cmd
docker run --net host --name mqtt-publisher -v /home/mr8/projects/grand_project/stormsmarthome/mqtt/data-file:/app/data-file mr4x2/mqtt-publisher:v1 node index.js -f data-file/house-0.csv -b 10.0.0.5 -s 100
```


**How to run multiple publisher -> broker**


Edit some variable env in [mqtt.env](./mqtt.env)

Add some services in docker-compose.yaml if need many traffic


How to run multiple container to test mqtt

```cmd
docker compose --env-file mqtt.env up -d
```
