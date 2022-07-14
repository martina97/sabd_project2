# Sistemi e Architetture per Big Data - AA 2021/2022

Lo scopo del progetto è rispondere ad alcune query riguardanti un dataset reale relativo a dati ambientali, provenienti da sensori messi a disposizione da Sensor.Community, una rete di sensori globale che ha l’obiettivo di fornire open data di tipo ambientale. Il dataset relativo ai dati ambientali contiene in particolare i valori di temperatura, altitudine e pressione misurati da sensori di tipo BMP180. Il dataset è fornito in formato CSV, con i campi separati dal carattere ; ed è disponibile all’indirizzo https://archive.sensor.community/csv_per_month/2022-05/2022-05_bmp180.zip.

Per eseguire l'applicazione, far partire i container tramite lo script "start-docker.sh" contenuto all'interno della cartella "docker". Per effettuare il submit del job con Flink, entrare nel container tramite :
```
sudo docker exec -t -i jobmanager /bin/bash
```
ed eseguire:
```
"flink run -c connectionToKafka.MyConsumer ./queries/sabd_project2-1.0-SNAPSHOT-jar-with-dependencies.jar".
```
Per generare la jar, eseguire il comando
```
mvn compile assembly:single
```
Una volta fatto partire il Consumer, eseguire tramite maven la il main della classe MyKafkaProducer.java, contenuta all'interno della directory https://github.com/martina97/sabd_project2/tree/master/src/main/java/kafka.
