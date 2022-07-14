package queries.query2;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import scala.Tuple2;
import utilities.Configurations;
import utilities.MyMetricSink;
import utilities.MyStringSerializationSchema;
import utilities.Sensor;

import static kafka.MyKafkaProducer.getFlinkPropAsProducer;

public class Query2 {
    public static void runQuery2(DataStream<Sensor> stream) throws Exception {

        KeyedStream<Sensor, Long> keyedStream = stream
                .keyBy(Sensor::getLocation);


        // FUNZIONA


        DataStreamSink<String> oneHourStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Q2Aggregate())
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(60)))
                .process(new Q2ProcessWindowFunction())
                //.map((MapFunction<String, String>) myOutput -> Q2Result.writeQuery2Result(myOutput, "OneHour"))
                .map(new MyMetricSink())
                //.addSink(new MetricSink());
                .addSink(new FlinkKafkaProducer<String>(Configurations.TOPIC2,
                        new MyStringSerializationSchema(Configurations.TOPIC2),
                        getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));


        DataStreamSink<String> oneDayStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(24),Time.hours(+22)))
                .aggregate(new Q2Aggregate())
                .windowAll(TumblingEventTimeWindows.of(Time.days(1),Time.hours(+22)))
                .process(new Q2ProcessWindowFunction())
                //.map((MapFunction<String, String>) myOutput -> Q2Result.writeQuery2Result(myOutput, "OneDay"))
                .map(new MyMetricSink())
                .addSink(new FlinkKafkaProducer<String>(Configurations.TOPIC2,
                        new MyStringSerializationSchema(Configurations.TOPIC2),
                        getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));





        DataStreamSink<String> oneWeekStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(168), Time.hours(70)))
                .aggregate(new Q2Aggregate())
                .windowAll(TumblingEventTimeWindows.of(Time.hours(168), Time.hours(70)))
                .process(new Q2ProcessWindowFunction())
                //.map((MapFunction<String, String>) myOutput -> Q2Result.writeQuery2Result(myOutput, "OneWeek"))
                .map(new MyMetricSink())
                .addSink(new FlinkKafkaProducer<String>(Configurations.TOPIC2,
                        new MyStringSerializationSchema(Configurations.TOPIC2),
                        getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));







        System.out.println("----sto in runQuery2");


    }
}
