package queries.query1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class Q1ProcessWindowFunction extends ProcessWindowFunction <ResultQuery1, ResultQuery1, Integer, TimeWindow>{

    @Override
    public void process(Integer key, Context context, Iterable<ResultQuery1> iterable, Collector<ResultQuery1> collector) throws Exception {

        System.out.println("start della window: "+context.window().getStart());

        ResultQuery1 res = iterable.iterator().next();
        Date date = new Date();
        date.setTime(context.window().getStart());
        System.out.println("date IN WINDOWFUNCTION = "+date);
        res.setTimestamp(date);
        System.out.println("---res: " + res);
        collector.collect(res);


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




