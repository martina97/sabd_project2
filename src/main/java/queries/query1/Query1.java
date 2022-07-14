package queries.query1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import utilities.*;
import utilities.Sensor;

import static kafka.MyKafkaProducer.getFlinkPropAsProducer;

public class Query1 {

    public static void runQuery1(DataStream<Sensor> stream) throws Exception {

        KeyedStream<Sensor, Integer> keyedStream = stream
                .filter(line -> line.getSensor_id()<10000)
                .keyBy(Sensor::getSensor_id);


        DataStreamSink<String> oneHourStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Q1Aggregate(),
                        new Q1ProcessWindowFunction())
                .map((MapFunction<Q1Result, String>) myOutput -> Q1Result.writeQuery1Result(myOutput, "OneHour"))
                .map(new MyMetricMap())
                .addSink(new FlinkKafkaProducer<String>(Configurations.TOPIC2,
                        new MyStringSerializationSchema(Configurations.TOPIC2),
                        getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));





        /*
         * unico modo per far iniziare la prima finestra il Sun May 01 00:00:00 CEST 2022
         * infatti, 168 ore sono una settimana, ma mettendo Time.days(7) la prima finestra iniziava il Thu Apr 28 02:00:00 CEST 2022,
         * io voglio che inizi il Sun May 01 00:00:00 CEST 2022, quindi 70 ore dopo il 28 Aprile alle 02:00
         */


        DataStreamSink<String> oneWeekStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(168), Time.hours(70)))
               .aggregate(new Q1Aggregate(), new Q1ProcessWindowFunction())
                .map((MapFunction<Q1Result, String>) myOutput -> Q1Result.writeQuery1Result(myOutput, "OneWeek"))
                .map(new MyMetricMap())
                .addSink(new FlinkKafkaProducer<String>(Configurations.TOPIC2,
                        new MyStringSerializationSchema(Configurations.TOPIC2),
                        getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));



        DataStreamSink<String> oneMonthStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.days(31), Time.hours(406)))
                .aggregate(new Q1Aggregate(), new Q1ProcessWindowFunction())
                .map((MapFunction<Q1Result, String>) myOutput -> Q1Result.writeQuery1Result(myOutput, "AllDataset"))
                .map(new MyMetricMap())
                .addSink(new FlinkKafkaProducer<String>(Configurations.TOPIC2,
                        new MyStringSerializationSchema(Configurations.TOPIC2),
                        getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));


        System.out.println("----sto in runQuery1");
    }


}
