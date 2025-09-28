# IOT-Data-API

## Run web app using docker container

### Build Image

#### Method 1: Use builded image from Docker Hub

duongtm3102/iot-data-api:v1

#### Method 2: Build your own image

```sh
cd ./webapp/iot-data-api

# For example, image name iot-data-api:v1
docker build -t iot-data-api:v1 .

```
### Run container

Create database credential file

```sh
cd ./webapp/
cp cred.json.example cred.json
```
Run container
```sh
docker run -p 9000:9000 -d --name iot-data-api --volume ./cred.json:/opt/app/cred.json iot-data-api:v1
```

### Or use docker compose
