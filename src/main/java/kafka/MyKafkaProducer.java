package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import utilities.Configurations;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;


public class MyKafkaProducer {

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configurations.KAFKA_BROKERS);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }


    public static void PublishMessages() throws IOException {


        final Producer<String, String> producer = createProducer();


        try {
            //stream verso file csv
            Long tsPrev = null; //timestamp precedente
            AtomicLong tsProva = new AtomicLong(-1); //timestamp precedente
            Stream<String> FileStream = Files.lines(Paths.get("data/"+ Configurations.datasetPath+".csv"));



            //rimozione dell'header e lettura del file
            FileStream.skip(1).forEach(line -> {



                //   System.out.println("------------------------START----------------------");

                String[] fields = line.split(";");
                String[] value = Arrays.copyOfRange(fields, 0, fields.length);
                String tsCurrent = value[5];    //timestamp linea corrente (in lettura)
                // System.out.println("value.length --> " + value.length);


                Long date = null;
                try {
                    date = dateFormat.parse(tsCurrent).getTime();
                } catch (ParseException ignored) { }


                // System.out.println("line: " + line);


                String dateStrKey = String.valueOf(date);
                // System.out.println("date == " + date + ", dateStrKey == " + dateStrKey);


                // System.out.println("aaa");
                //invio dei messaggi
                ProducerRecord<String, String> CsvRecord = new ProducerRecord<>( Configurations.TOPIC1, 0, date, dateStrKey, line);
                //   System.out.println("bbb");



                //replay
                if (Configurations.replay == true)  {
                    if (tsProva.get() != -1) {
                        // replay
                        Long diffTS = date - tsProva.get();
                        System.out.println("diffTS == " + diffTS);

                        try {
                            Thread.sleep(diffTS);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }

                    } else {
                        //no replay, perche e' il primo record, quindi non faccio niente

                    }
                }



                /*
                if (value.length != 7 && value.length != 10 ) {
                    System.out.println(line);
                    System.out.println("len diversa ");
                }

                 */


                //se temperatura e' null, non invio i dati
                if (value.length == 10) {
                    //invio record
                    producer.send(CsvRecord, (metadata, exception) -> {
                        //System.out.println("ccc");
                        if(metadata != null){
                            //System.out.println("ddd");
                            //successful writes
                            System.out.println("CsvData: -> "+ CsvRecord.key()+" | "+ CsvRecord.value());
                        }
                        else{
                            //unsuccessful writes
                            System.out.println("Error Sending Csv Record -> "+ CsvRecord.value());
                        }
                    });
                }


                //System.out.println("date prima tsProva == " + date);
                tsProva.set(date);
                //System.out.println("tsProva == "  + tsProva.get()+"  "+ new Date(tsProva.get()));
                //System.out.println("------------------------END----------------------");


            });

        } catch (IOException e) {
            e.printStackTrace();
        }
        // provo a mandare un ultimo stream fittizio in modo che mi prende anche tutto il mese di maggio, quindi ad esempio 1 giugno 01:00
        String dummyTimestamp = "2022-06-01T01:00:00";
        String dummyLine = "5571;BMP180;2808;51.240;6.794;2022-06-01T01:00:00;101910;;;10.7";
        System.out.println("dummyTimestamp " + dummyTimestamp);
        Long date = null;
        try {
            date = dateFormat.parse(dummyTimestamp).getTime();
        } catch (ParseException ignored) { }
        String dateStrKey = String.valueOf(date);
        System.out.println("date == " + date + ", dateStrKey == " + dateStrKey);
        ProducerRecord<String, String> CsvRecord = new ProducerRecord<>( Configurations.TOPIC1, 0, date, dateStrKey, dummyLine);
        //invio record
        producer.send(CsvRecord, (metadata, exception) -> {
            System.out.println("ccc");
            if(metadata != null){
                System.out.println("ddd");
                //successful writes
                System.out.println("CsvData: -> "+ CsvRecord.key()+" | "+ CsvRecord.value());
            }
            else{
                //unsuccessful writes
                System.out.println("Error Sending Csv Record -> "+ CsvRecord.value());
            }
        });

        producer.close();

    }



    //metodo che crea proprietà per creare sink verso kafka
    public static Properties getFlinkPropAsProducer(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configurations.KAFKA_BROKERS);
        //properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, Configurations.CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        return properties;
    }


    public static void main(String[] args) throws IOException {

        PublishMessages();

    }
}
