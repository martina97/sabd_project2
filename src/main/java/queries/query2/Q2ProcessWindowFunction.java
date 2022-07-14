package queries.query2;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

public class Q2ProcessWindowFunction extends ProcessAllWindowFunction<Q2Result, String, TimeWindow> {
        private TreeMap<Double, List<Long>> rankMap;    //key = temp, key=list<location>


        @Override
        public void process(Context context, Iterable<Q2Result> iterable, Collector<String> collector) throws Exception {
                System.out.println("\n\n -------------- start della window: "+context.window().getStart() + "--------------");
                Date date = new Date();
                date.setTime(context.window().getStart());
                System.out.println("date == " + date);

                rankMap = new TreeMap<>(Collections.reverseOrder());

                for (Q2Result res : iterable) {
                        Double key = res.getAvg_temperature();
                        if (!rankMap.containsKey(key)) {
                                List<Long> listLocation = new ArrayList<>();
                                listLocation.add(res.getLocation());
                                rankMap.put(key,listLocation);

                        } else {
                                List<Long> listLocation = rankMap.get(key);
                                listLocation.add(res.getLocation());
                                rankMap.put(key, listLocation);
                        }
                        //System.out.println("res ----> " + res);
                }
                Q2Result res = iterable.iterator().next();

                res.setTimestamp(date);
                //System.out.println("rankMap --> " + rankMap);

                //System.out.println(" prova rank ---> " + rankMap.entrySet().stream().limit(2).collect(Collectors.toList()));
                List<Map.Entry<Double, List<Long>>> top5High = rankMap.entrySet().stream().limit(5).collect(Collectors.toList());
                //System.out.println("top5High --> " + top5High);

               /*
                System.out.println(" prova rank ---> " + rankMap.entrySet().stream().limit(2).collect(toMap(Map.Entry::getKey, Map.Entry::getValue,
                        (v1,v2) -> v1, TreeMap::new)));
                System.out.println(" prova rank ---> " + rankMap.entrySet().stream().limit(2).collect(Collectors.toSet()));

                */
                List<Map.Entry<Double, List<Long>>> top5Low = rankMap.descendingMap().entrySet().stream().limit(5).collect(Collectors.toList());
               // System.out.println("top5Low --> " + top5Low);
                top5High.addAll(top5Low);
                //System.out.println("tot --> " + top5High);
                //System.out.println(" prova rank ---> " + rankMap.values().stream().limit(2).collect(Collectors.toList()));
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

                String ts= simpleDateFormat.format(res.getTimestamp());
                String result = ts + ";"+top5High.get(0) + ";"+ top5High.get(1)+ ";"+top5High.get(2)+ ";"+top5High.get(3)+ ";"+top5High.get(4)+ ";"+
                        top5High.get(5)+ ";"+top5High.get(6)+ ";"+top5High.get(7)+ ";"+top5High.get(8)+ ";"+top5High.get(9);
                collector.collect(result);
        }


}


