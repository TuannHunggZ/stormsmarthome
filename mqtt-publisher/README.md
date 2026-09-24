# DEBS 2014 MQTT Replay

CLI Node.js để replay file CSV của DEBS 2014 lên MQTT broker theo đúng đặc tính thời gian trong dataset.

## Cài đặt

```bash
npm install
```

## Lấy data DEBS 2014

Dataset DEBS 2014 có thể được tải xuống từ Google Drive. Hãy làm theo các bước sau:

```bash
mkdir -p data-file
python3 -m venv venv
source venv/bin/activate
pip install gdown
gdown 14nO_NhyyJ_ig25RqvTS4wrkm-Zv1revR
tar -xzf debs40houses16h.tar.gz -C data-file/
```

Dataset sẽ được giải nén vào thư mục `data-file/`, chứa 40 file CSV (`house-0.csv` đến `house-39.csv`), mỗi file đại diện cho một ngôi nhà.

## Chạy

```bash
node src/main.js --file data-file/house-0.csv --topic iot-data
```

## Tùy chọn

- `--broker <host|url>`: broker MQTT, mặc định `localhost`
- `--port <number>`: port MQTT, mặc định `1883`
- `--topic <topic>`: topic publish
- `--qos <0|1|2>`: MQTT QoS
- `--retain`: bật retain flag
- `--speed-factor <number>`: tăng tốc replay
- `--use-current-time`: thay timestamp trong payload bằng Unix timestamp hiện tại

## Kiến trúc

- `CsvReplayReader`: đọc CSV bằng stream và trả về từng nhóm record cùng timestamp
- `ReplayScheduler`: tính delay giữa các timestamp và sleep
- `MqttPublisher`: kết nối và publish MQTT
- `StatisticsReporter`: in thống kê mỗi giây
- `main`: ghép các thành phần lại với nhau