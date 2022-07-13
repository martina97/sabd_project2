# sabd_project2

rimuovere topic
'''
sudo docker exec docker_kafka1_1 kafka-topics --list --zookeeper zookeeper:2181
sudo docker exec docker_kafka1_1 kafka-topics --delete --zookeeper zookeeper:2181 --topic NOMETOPIC

mvn compile assembly:single
sudo docker exec -t -i jobmanager /bin/bash
flink run -c kafka.MyConsumerKafka ./queries/sabd.jar
flink run -c kafka.MyKafkaConsumer ./queries/sabd_project2-1.0-SNAPSHOT-jar-with-dependencies.jar
'''