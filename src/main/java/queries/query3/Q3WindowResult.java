package queries.query3;

import org.apache.flink.api.java.tuple.Tuple2;
import utilities.Configurations;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Q3WindowResult {

    private TreeMap<Integer, Tuple2<Double,Double>> cells;
    private Date timestamp;

    public Q3WindowResult(TreeMap<Integer, Tuple2<Double, Double>> cells, Date timestamp) {
        this.cells = cells;
        this.timestamp = timestamp;
    }

    public static String writeQuery3Result(Q3WindowResult myOutput, String evenTime) throws FileNotFoundException {

        StringBuilder sb = new StringBuilder();
        Date timestamp = myOutput.getTimestamp();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        sb.append(simpleDateFormat.format(timestamp));

        sb.append(",");


        TreeMap<Integer, Tuple2<Double,Double>> cells = myOutput.getCells();
        Set<Integer> boh = cells.keySet();

        for (int j=0; j<16; j++) {
            if (!boh.contains(j)) {
                cells.put(j,new Tuple2<>(0.0,0.0));
            }
        }


        for (Map.Entry<Integer, Tuple2<Double,Double>> entry : cells.entrySet()) {

                sb.append(entry.getKey());
                sb.append(",");
                sb.append(entry.getValue().f0);
                sb.append(",");
                sb.append(entry.getValue().f1);
            // sb.append(",");


            }

        return sb.toString();


    }


    public static String writeQuery3ResultCSV(Q3WindowResult myOutput, String evenTime) throws FileNotFoundException {
        String outputPath = "./Results/QUERY3_"+evenTime+".csv";
        System.out.println("outputPath: "+outputPath);
        //System.out.println("SONO IN WRITE QUERY2 ------ \n line === " + myOutput);

        PrintWriter writer = new PrintWriter(new FileOutputStream(outputPath, true));

        StringBuilder sb = new StringBuilder();
        Date timestamp = myOutput.getTimestamp();
        //SimpleDateFormat simpleDateFormat = new SimpleDateFormat(("yyyy-MM-dd"));
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        sb.append(simpleDateFormat.format(timestamp));

        sb.append(",");


        TreeMap<Integer, Tuple2<Double,Double>> cells = myOutput.getCells();
        Set<Integer> boh = cells.keySet();

        for (int j=0; j<16; j++) {
            if (!boh.contains(j)) {
                cells.put(j,new Tuple2<>(0.0,0.0));
            }
        }


        for (Map.Entry<Integer, Tuple2<Double,Double>> entry : cells.entrySet()) {

            sb.append(entry.getKey());
            sb.append(",");
            sb.append(entry.getValue().f0); //todo: temp?
            sb.append(",");
            sb.append(entry.getValue().f1); //todo: ?
            if (entry.getKey() != 15) {
                sb.append(",");
            } else {
                sb.append("\n");
            }

        }



        writer.write(sb.toString());
        writer.flush();
        writer.close();

        return sb.toString();


    }

    @Override
    public String toString() {
        return "Q3WindowResult{" +
                "cells=" + cells +
                ", timestamp=" + timestamp +
                '}';
    }

    public TreeMap<Integer, Tuple2<Double, Double>> getCells() {
        return cells;
    }

    public void setCells(TreeMap<Integer, Tuple2<Double, Double>> cells) {
        this.cells = cells;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
}
