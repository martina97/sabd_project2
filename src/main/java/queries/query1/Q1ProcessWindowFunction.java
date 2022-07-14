package queries.query1;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

public class Q1ProcessWindowFunction extends ProcessWindowFunction <Q1Result, Q1Result, Integer, TimeWindow>{

    @Override
    public void process(Integer key, Context context, Iterable<Q1Result> iterable, Collector<Q1Result> collector) throws Exception {

        System.out.println("start della window: "+context.window().getStart());

        Q1Result res = iterable.iterator().next();
        Date date = new Date();
        date.setTime(context.window().getStart());
        System.out.println("date IN WINDOWFUNCTION = "+date);
        res.setTimestamp(date);
        //System.out.println("---res: " + res);
        collector.collect(res);

    }
}



