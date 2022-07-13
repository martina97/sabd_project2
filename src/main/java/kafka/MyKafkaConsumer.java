package kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import queries.query1.Query1;
import queries.query2.Query2;
import queries.query3.Query3;
import utilities.Configurations;
import utilities.Sensor;

import java.time.Duration;
import java.util.Properties;

public class MyKafkaConsumer {
    //static ArrayList<Cell> grid;
    public static void main(String[] args) throws Exception {

        /*
        System.out.println(" ---- prima grid ---- ");
        ArrayList<Cell> grid = Query3.createGrid();
        System.out.println(" ---- dopo grid ---- ");

        System.out.println("grid ==== " + grid);

         */

        FlinkKafkaConsumer<String> consumer = createConsumer();
        consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(1)));
        StreamExecutionEnvironment env = createEnviroment();



        //creo lo stream di tipo "Sensor" andando a splittare le righe
        //che vengono lette dal topic di kafka da parte del consumer con MyMapFunction
        DataStream<Sensor> stream = env.addSource(consumer)
                .setParallelism(1)
                .map(new MapFunction<String, Sensor>() {
                    @Override
                    public Sensor map(String line) throws Exception {
                        String[] values = line.split(";");
                        System.out.println("line DENTRO MAP== " + line);

                        //ArrayList<Cell> grid2 = grid;

                        Sensor sensor = new Sensor(Integer.valueOf(values[0]),
                                values[1],
                                Long.valueOf(values[2]),
                                //Double.valueOf(values[3]),
                                //Double.valueOf(values[4]),
                                Sensor.checkOutliersLatLon(values[3]),
                                Sensor.checkOutliersLatLon(values[4]),
                                values[5],
                                Double.valueOf(values[6]),
                                Double.valueOf(values[9]));

        /*
        System.out.println("sensor id  =  " + sensor.getSensor_id() + ", sensor type = " + sensor.getSensor_type() + " location =" + sensor.getLocation() +
               " lat = " + sensor.getLat() + ", lon = " + sensor.getLon() + ", timestamp = " + sensor.getTimestamp() + ", pressure = " + sensor.getPressure() +
                ", altitude = " + sensor.getAltitude() + ", pressure_sealevel = " + sensor.getPressure_sealevel() + ", temperature = " + sensor.getTemperature());
        */
                        System.out.println(sensor);
                        //System.out.println(" ------ SONO IN MY MAP ----");
                        //System.out.println("grid ======== " + grid2);
                        sensor.setCell();
                        return sensor;
                    }
                })
                .filter(sensor -> sensor.getTemperature()>-40.0 && sensor.getTemperature()<85.0);





        //Query1.runQuery1(stream);
        //Query2.runQuery2(stream);
        Query3.runQuery3(stream);

        env.setParallelism(3);
        env.execute("sabd2");


    }
    /*
    public static class MyMapFunc implements MapFunction<String, ArrayList<Cell> , Sensor> {


        @Override
        public Sensor map(String line,  ArrayList<Cell> grid) throws Exception {

            String[] values = line.split(";");
            System.out.println("line DENTRO MAP== " + line);

            Sensor sensor = new Sensor(Integer.valueOf(values[0]),
                    values[1],
                    Long.valueOf(values[2]),
                    //Double.valueOf(values[3]),
                    //Double.valueOf(values[4]),
                    Sensor.checkOutliersLatLon(values[3]),
                    Sensor.checkOutliersLatLon(values[4]),
                    values[5],
                    Double.valueOf(values[6]),
                    Double.valueOf(values[9]));


            System.out.println(sensor);
            System.out.println(" ------ SONO IN MY MAP ----");
            System.out.println("grid ======== " + grid);
            sensor.setCell(grid);
            return sensor;
        }

    }

     */

    public static FlinkKafkaConsumer<String> createConsumer() throws Exception {
        // creazione properties
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configurations.KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerGroup");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // creazione consumer usando le properties
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(Configurations.TOPIC1, new SimpleStringSchema(), props);

   /*
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(Configurations.TOPIC1, new SimpleStringSchema() {
            private static final long serialVersionUID = 1L;
            private final String SHUTDOWN = "SHUTDOWN";

            @Override
            public String deserialize(byte[] message) {

                return super.deserialize(message);
            }

            @Override
            public boolean isEndOfStream(String nextElement) {
                if (SHUTDOWN.equalsIgnoreCase(nextElement)) {
                    System.out.println("cccccccccccccccc");
                    return true;
                }
                System.out.println("aaaaaaaaaaaaaaaa");
                return super.isEndOfStream(nextElement);
            }
        } , props);

    */


        System.out.println("---creato consumer--");
        return myConsumer;

    }

    public static StreamExecutionEnvironment createEnviroment(){
        System.out.println("--sto in create env--");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        return env;
    }


}
