## Simulate Streaming Fraud Detection 

#### Overview 
![final-flow](https://github.com/user-attachments/assets/3ddf0ca4-0de7-4940-af41-a9fef2e8c8f9)


This project is a real-time streaming pipeline for detecting frudulent transactions. It integrates multiple technologies startin from a user interface to a visual monitoring dashboard


#### Components 
1. Flask UI
   - A simple web form for submitting transactions.
   - Sends transaction data to RabbitMQ via HTTP POST
2. RabbitMQ
   - A message broker used to queue incoming transactions.
3. Java Consumer (RabbitMQ -> Kafka)
   - A java application that listens to a RabbitMQ queue.
   - Reads transaction messages and forwards them to a Kafka topic.
4. Apache Kafka
   - A distributed streaming platform used to ingest transaction data.
   - Topics:
       - `transactions-stream` (raw transactions)
       - `blacklist_devices_list` (dictionary for blacklisted devices)
       - `blacklist_customers_list` (dictionary for blacklisted customers)
       - `big_amount`  (fraud-detected big amount transactions made by flink)
       - `blacklist`  (fraud-detected blacklist transactions made by flink)
       - `device_alerts` (fraud-detected device alert transactions made by flink)
       - `velocity_alerts` (fraud-detected device alert transactions made by flink)
       - `geolocation_alerts` (fraud-detected device alert transactions made by flink)
5. Apache Flink (Fraud Detection Engine)
   - Processes transactions in real time and checks for fraud patterns susch as:
       - Unusual transaction amount save  in topic kafka `big_amount`
       - Geolocation anomaly, transactions are made from 2 or more different locations in < 5 minutes save in topic kafka `geolocation_alerts`
       - Device incosistency, device_id used by > 3 different customers save in topic kafka `device_alerts`
       - blacklist customers and device, detect transaction by  blacklist customers or blacklist device, list blacklist customers save in `blacklist_customers_list` and list blacklist device save in `blacklist_devices_list`  and then fraud blacklist save in topic kafka `blacklist`
       - Transactions velocity rule, 5 transactions or more in 1 minute by 1 user
6. Connector Kafka Connect Clickhouse
   - sink topic `transactions-stream`, `big_amount`, `blacklist`, `device_alerts`, `geolocation_alerts`, `velocity_alerts` to clickhouse table
7. ClickHouse
   - A high-performance analytical columnar database
   - Stores enriched transaction data.
   - Table: fraud_transactions.
8. Grafana
   - Connects to ClickHouse as a data source.
   - Visualizes fraud statistics and alerts on a real-time dashbord.


### Project Workflow 
1. Flask Ui
   In this Project Using Flask for Web UI Transactions form for input data transaction like customer_id, amount, merchant, location, payment_method, and device_id. in backend flask will send data input user into RabbitMQ. file code flask in `./app.py` and file html in `./templates/index.html`
3. RabbitMQ
   RabbitMQ in this project use for broker message queue that save data, and in this project using docker for RabbitMQ
5. Java Consumer RabbitMQ & Producer Kafka
   this Jar will consume data in RabbitMQ and produce data to kafka topics the jar in `./consumerRabbitMQKafka-1.0-SNAPSHOT.jar` this jar will read configuration file `./configRabbitMQ.properties`. You can check or edit code java in directory `./consumerRabbitMQKafka/` 
7. Kafka
   In this Project Using Local Windows Kafka with default configuration, you can check the kafka configuration in `./kafka/`
9. Apache Flink
    All Job and lib for Apache FLink in `./flink-job/` and in this folder is volumes mapping for Docker Flink.
11. Connector Kafka Connect Clickhouse
    This project using kafka connect for sink into clickhouse and we also need jar connector clickhouse for this, because in default kafka connect there is no connector clickhouse, and i also add jar connector clickhouse in `./lib/` this dictionary also made volumes mapping for kafka connector docker. the configuration for sink each table in `./connector-configuration/`
13. Clickhouse
    This Project Using Clickhouse for database, and clickhouse running in docker and we need create table transaction and table table type fraud, you can check in `./table-clickhouse.sql` for query create table.
15. Grafana
    this project i make simple dashbord for monitoring fraud streaming detection in Grafana

#### Steps 
1. clone this project
2. Running Kafka Local or if you use docker just run it
3. Up all container docker  in docker compose file `./docker-compose.yaml` with `docker-compose up` wait unting all container running properly
4. install all depedency in `./requirements.txt` with `pip install -r requirements.txt`
5. running flask in development test with `python app.py`
6. running jar `./consumerRabbitMQKafka-1.0-SNAPSHOT.jar` with `java -jar consumerRabbitMQKafka-1.0-SNAPSHOT.jar`
7. Set up flink or running all job for Fraud Detection Engine
   First enter into jobmanager container with `docker exec -it jobmanager bash`
     - Running Job Fraud Engine for big_amount with `flink run -py /opt/flink/job/big_amount.py` this job will find or check transaction that have amount > 10000000 and save record in kafka topic `big_amount`
     - Running Job Fraud Engine for blacklist customers  `flink run -py /opt/flink/job/blacklist_customers.py` this job will read blacklist customers in topic kafka `blacklist_customers_list` and find transaction from customer_id in blacklist customer and then save record in topic `blacklist`
     - Running Job Fraud Engine for blacklist devices `flink run -py /opt/flink/job/blacklist_devices.py` this job wll read blacklist devices in topic topic kafka `blacklist_devices_list` and find transaction from devices_id in blacklist devices and then save record in topic `blacklist`
     - Running Job Fraud Engine for Device Alerts  `flink run -py /opt/flink/job/device_alerts.py` this job will check transaction that use device_Id with > 3 different customers in 5 minutes and save records in topic kafka `device_alerts`
     - Running Job Fraud Engine for geolocation alerts  `flink run -py /opt/flink/job/geolocation_alerts.py` this job will check customers that make transaction in 2 or more different location in 5 minutes and then save records in topic kafka `geolocation_alerts`
     - Running Job Fraud Engine for Velocity rule alerts `flink run -py /opt/flink/job/velocity_alerts.py` this job will find 5 transactions or more by 1 customers_id in 5 minutes and save records in topic kafka `velocity_alerts`
8. Next step is sink topic kafka into clickhouse, the topics that will be sent to kafka are:
   - `transactions-stream` on clickhouse table `transactions`
   - `big_amount` on clickhouse table `big_amount`
   - `blacklist` on clickhouse table `blacklist`
   - `geolocation_alerts` on clickhouse table `geolocation_alerts`
   - `device_alerts` on clickhouse table `device_alerts`
   - `velocity_alerts` on clickhouse table `velocity_alerts`
   we need json configuration file connector for each topic that sink to clickhouse and you can check all file in `./connector-configurations/` and then you can run all configuration file with postman or with command
```bash
curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d @transactions.json
```
9. After that make sure the data already available in table clickhouse. and the next step is make dashbord in grafana. in grafana you can add data source clickhouse and make visualization dashbord in there
10. and in bellow the demo from user input transaction fraud big amount and the data streaming in dashbord grafana

https://github.com/user-attachments/assets/0b860d8c-c39f-48a2-ae39-55665df90c9c



   
