package utilities;

public interface Configurations {
    //String KAFKA_BROKERS = "kafka1:29092";    //TODO
    String KAFKA_BROKERS = "localhost:9092";    //TODO
    String CLIENT_ID = "myclient";
    String datasetPath = "prova";
   //String datasetPath = "2022-05_bmp180";
    String TOPIC1 = "source";
    String TOPIC2 = "results";

    Boolean replay = false;
}
