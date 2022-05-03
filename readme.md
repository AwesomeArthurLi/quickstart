
# flink-lab-hybrid

## version
- JDK

11.0.12 (x86_64) "Oracle Corporation" - "Java SE 11.0.12"

- Scala

Scala code runner version 2.12.14

- kafka

2.13-3.1.0

- OS

MacOS Monterey

## code
- HelloHybrid.scala

you need to adjust the code

- flat file

sensor.csv


## kafka

- create the topic
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092  --create --topic lab-flink-sensor-iot
```
-  list the topic
```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092  --list
```

- produce message
```bash
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic lab-flink-sensor-iot
```

- consume message
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092  --topic lab-flink-sensor-iot --from-beginning
```

## flame graph

hybrid flame graph.html
