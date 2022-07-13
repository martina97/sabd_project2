package queries.query3;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import queries.query2.Q2Result;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class Q3ProcessWindowFunction extends ProcessAllWindowFunction<Q3Result, Q3WindowResult, TimeWindow> {
        private TreeMap<Integer, Tuple2<Double,Double>> cells;    //key = temp, key=list<location>


        @Override
        public void process(ProcessAllWindowFunction<Q3Result, Q3WindowResult, TimeWindow>.Context context, Iterable<Q3Result> iterable, Collector<Q3WindowResult> collector) throws Exception {
                System.out.println("\n\n -------------- start della window: "+context.window().getStart() + "--------------");
                Date date = new Date();
                date.setTime(context.window().getStart());
                System.out.println("date == " + date);

                cells = new TreeMap<>();

                for (Q3Result res : iterable) {
                        int numCell = Integer.valueOf(res.getCell().getIdCell().replace("cell_","")); // ho int = num cella
                        Tuple2<Double,Double> value = new Tuple2<>(res.getAvg_temperature(), res.getMedianHeap());
                        cells.put(numCell,value);

                }
                Q3Result res = iterable.iterator().next();

                res.setTimestamp(date);

                Q3WindowResult finalRes = new Q3WindowResult(cells, date);
                System.out.println("finalRes --> " + finalRes);


                collector.collect(finalRes);
        }
}


 /*

    @Override
    public void process(String key, Context context, Iterable<Tuple2<String,Double>> values, Collector<Tuple2<String,Double>> out) throws Exception {


        System.out.println("------------------------STO IN PROCESS !!!!!!!!!!!!!!!------------------------");
        Tuple2<String,Double> avg = values.iterator().next();
        out.collect(new Tuple2<>(avg.f0, avg.f1));
        /*
        for (Tuple2<String,Double> value : values) {
            System.out.println("-=-= tupla: "+value.f0+", "+value.f1);
        }


        Tuple2<String,Double> result = values.iterator().next();
        //result.setTemperature(sum / count);
        out.collect(result);

         */




