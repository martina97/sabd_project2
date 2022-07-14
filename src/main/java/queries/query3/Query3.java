package queries.query3;
/*
Considerare le coordinate di latitudine e longitudine all'interno dell'area geografica che è identificata
dalle coordinate di latitudine e longitudine (38°, 2°) e (58°, 30°).

Dividere quest'area utilizzando una griglia 4x4 e identificare ciascuna cella della griglia dall'angolo
in alto a sinistra a quello in basso a destra utilizzando il nome "cella_X", dove X è l'id della cella da 0 a 15.
Per ogni cella, trovare la temperatura media e mediana, tenendo conto dei valori emessi dai sensori che
si trovano all'interno di quella cella
 */

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import utilities.*;


import java.util.ArrayList;

import static kafka.MyKafkaProducer.getFlinkPropAsProducer;

public class Query3 {

    static ArrayList<Cell> grid = new ArrayList<>();

    public static void runQuery3(DataStream<Sensor> stream) throws Exception {

        KeyedStream<Sensor, String> keyedStream = stream
                .filter(line -> line.getCell() != null)
                .keyBy(line -> line.getCell().getIdCell());


/*
        DataStreamSink<String> oneHourStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Q3Aggregate())
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new Q3ProcessWindowFunction())
                .map((MapFunction<Q3WindowResult, String>) myOutput -> Q3WindowResult.writeQuery3Result(myOutput, "OneHour"))
                .map(new MapFuncProva())
                //.map(new QueryLatencyTracker())
                .addSink(new FlinkKafkaProducer<String>(Configurations.TOPIC2,
                        new MyStringSerializationSchema(Configurations.TOPIC2),
                        getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));



        DataStreamSink<String> oneDayStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.days(1),Time.hours(+22)))
                .aggregate(new Q3Aggregate())
                .windowAll(TumblingEventTimeWindows.of(Time.days(1),Time.hours(+22)))
                .process(new Q3ProcessWindowFunction())
                .map((MapFunction<Q3WindowResult, String>) myOutput -> Q3WindowResult.writeQuery3Result(myOutput, "OneHour"))
                .map(new MapFuncProva())
                .addSink(new FlinkKafkaProducer<String>(Configurations.TOPIC2,
                        new MyStringSerializationSchema(Configurations.TOPIC2),
                        getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

 */





        DataStreamSink<String> oneWeekStream = keyedStream
                .window(TumblingEventTimeWindows.of(Time.hours(168), Time.hours(70)))
                .aggregate(new Q3Aggregate())
                .windowAll(TumblingEventTimeWindows.of(Time.hours(168), Time.hours(70)))
                .process(new Q3ProcessWindowFunction())
                .map((MapFunction<Q3WindowResult, String>) myOutput -> Q3WindowResult.writeQuery3Result(myOutput, "OneHour"))
                .map(new MyMetricSink())
                .addSink(new FlinkKafkaProducer<String>(Configurations.TOPIC2,
                        new MyStringSerializationSchema(Configurations.TOPIC2),
                        getFlinkPropAsProducer(),
                        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));


        System.out.println("----sto in runQuery3");


    }

    public static ArrayList<Cell> createGrid() {
        /*

        (58,2) -------- (58,9) -------- (58,16) -------- (58,23) -------- (58,30)
           ╎   cell_0     ╎     cell_1      ╎     cell_2    ╎      cell_3    ╎
        (53,2) -------- (53,9) -------- (53,16) -------- (53,23) -------- (53,30)
           ╎   cell_4     ╎     cell_5      ╎     cell_6     ╎      cell_7   ╎
        (48,2) -------- (48,9) -------- (48,16) -------- (48,23) -------- (48,30)
           ╎   cell_8     ╎     cell_9      ╎     cell_10    ╎     cell_11   ╎
        (43,2) -------- (43,9) -------- (43,16) -------- (43,23) -------- (48,30)
           ╎   cell_12    ╎    cell_13     ╎     cell_14    ╎     cell_15    ╎
        (38,2) -------- (38,9) -------- (38,16) -------- (38,23) -------- (38,30)


        */
        ArrayList<Cell> grid = new ArrayList<>();
        Double lat1 = 58.000;
        Double lat2 = 38.000;
        Double lon1 = 2.000;
        Double lon2 = 30.000;
        Double stepLon = (lon2-lon1)/4;
        Double stepLat = (lat1-lat2)/4;
        //System.out.println("stepLon === " + stepLon);
        //System.out.println("stepLat === " + stepLat);

        /*
        cella_0:
         (58,2) --- (58,7)
           ╎          ╎
         (53,2) --- (53,7)
        */

        for (int i = 0; i<16; i++) {

            String cellName = "cell_" + i;
            Cell cell = new Cell(lat1,lon1,lat1-stepLat,lon1+stepLon, cellName);
            grid.add(cell);
           // System.out.println(cell);
           // System.out.println("lat1 ==" + lat1 + " lon1 ==" + lon1);
            if (lon1<lon2-stepLon) {
                lon1 += stepLon;
            } else {
                // ricomincio da sx
                lat1 -= stepLat;
                lon1 = 2.000;
            }

            //System.out.println("\n#########################\n\n");

        }

        return grid;

    }
}
