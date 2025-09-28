## MQTT broker tutorial

### Run MQTT broker

Step 1: Build docker image
```cmd
docker build -t mqtt-broker .
```

Step 2: Run a docker container
```cmd
docker run -d -p 1883:1883 --name mqtt mqtt-broker
```
