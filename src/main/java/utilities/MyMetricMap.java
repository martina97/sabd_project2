package utilities;

import org.apache.flink.api.common.functions.MapFunction;

public class MyMetricSink implements MapFunction<String, String> {
    private static long count = 0L;
    private static long startTime = 0L;


    @Override
    public String map(String s) throws Exception {
        if (startTime == 0L) {
            startTime = System.currentTimeMillis();
            //System.out.println("---vista prima tupla");
            return s;
        }


        count++;
        //tempo corrente
        double currentTime = System.currentTimeMillis() - startTime;
        currentTime = currentTime/1000;

        double throughput = (count/currentTime);
        double latency = (currentTime/count);

        //throughput e latenza calcolate finora
        System.out.println("throughput: " + throughput);
        System.out.println("latency: " + latency);

        String res = s + ","+throughput + "," + latency;
        System.out.println("res ======= " + res);

        return res;
    }
}
