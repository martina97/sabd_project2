package utilities;

import org.apache.flink.api.common.functions.MapFunction;

import java.io.FileWriter;

public class MapFuncProva implements MapFunction<String, String> {
    private static long count = 0L;
    private static long startTime = 0L;


    @Override
    public String map(String s) throws Exception {
        if (startTime == 0L) {
            startTime = System.currentTimeMillis();
            System.out.println("---vista prima tupla");
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

        /*
        //FileWriter csvWriter = new FileWriter("./docker/jobmanager_volume/metrics.txt");
        //FileWriter csvWriter = new FileWriter("./docker/volumes/metrics.txt");
        FileWriter csvWriter = new FileWriter("metrics.txt");

        // String outputPath = "/docker/jobmanager_volume/metrics.txt";
        // System.out.println("outputPath: "+outputPath);
        // PrintWriter writer = new PrintWriter(new FileOutputStream(outputPath, true));

        // StringBuilder sb = new StringBuilder();
        //SimpleDateFormat simpleDateFormat = new SimpleDateFormat(("yyyy-MM-dd"));
        csvWriter.append("throughput == " + throughput);
        csvWriter.append(",");
        csvWriter.append("latency == " + latency);
        csvWriter.append("\n");
        csvWriter.flush();
        csvWriter.close();

         */

        return res;
    }
}
