# JVM module

Source code with jvm applications for streaming

## How to produce data in Apache Kafka
```
cd ../python
python3 -m virtualenv venv
source venv/bin/activate
pip install poetry
poetry install
cd apps
python kafka_producer.py
```

## How to check work of jobs
```
cd jvm/flink-processing
mvn clean install
java -jar *path to result jar*
```