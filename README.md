# streaming

Repository with examples of some streaming applications

## How to launch required services

### Kafka
```
docker-compose up -d zookeeper kafka
```

### Spark 3.1.2 with Python
```
docker-compose up -d spark-python
```

### Flink
```
docker-compose up -d flink-task-manager flink-job-manager zeppelin
```
