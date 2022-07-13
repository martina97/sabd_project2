#!/bin/bash

#sudo docker-compose build
sudo docker-compose up -d

# remove topic
#sudo docker exec docker_kafka-broker_1 kafka-topics --list --zookeeper zookeeper:2181
#sudo docker exec docker_kafka-broker_1 kafka-topics --delete --zookeeper zookeeper:2181 --topic flink-events5
#echo "--- topic removed ---"