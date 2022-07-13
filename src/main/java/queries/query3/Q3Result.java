package queries.query3;

import utilities.Cell;
import utilities.Configurations;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Q3Result {
    //private HashMap<Long,Double> map;

    private Cell cell;
    private Double avg_temperature;
    private Double median;
    private Long count;
    private Date timestamp;







    public Double getMedian() {
        return median;
    }

    public void setMedian(Double median) {
        this.median = median;
    }

    public Double getMedianHeap() {
        return medianHeap;
    }

    public void setMedianHeap(Double medianHeap) {
        this.medianHeap = medianHeap;
    }

    private Double medianHeap;

    public Q3Result(Cell cell, Double temperature, Double median, Double medianHeap, Long occurrences) {
        this.cell = cell;
        this.avg_temperature = temperature;
        this.median = median;
        this.medianHeap = medianHeap;
        this.count = occurrences;

    }



    @Override
    public String toString() {
        return "Q3Result{" +
                "cell=" + cell.getIdCell() +
                ", avg_temperature=" + avg_temperature +
                ", median=" + median+
                ", medianHeap=" + medianHeap+
                ", count=" + count +
                ", timestamp=" + timestamp +
                '}';
    }

    public Cell getCell() {
        return cell;
    }

    public void setCell(Cell cell) {
        this.cell = cell;
    }

    public Double getAvg_temperature() {
        return avg_temperature;
    }

    public void setAvg_temperature(Double avg_temperature) {
        this.avg_temperature = avg_temperature;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
/*
    public static String writeQuery1Result(ResultQuery1 myOutput, int days) throws FileNotFoundException {
        //scrive questo (esempio): 3> 2015-04-09,C20,army,0.29,others,0.57

        String outputPath = "results/"+Config.datasetPath+"_"+days+"_QUERY1_MARTI.csv";
        System.out.println("outputPath: "+outputPath);
        PrintWriter writer = new PrintWriter(new FileOutputStream(outputPath, true));

        StringBuilder sb = new StringBuilder();
        Date timestamp = myOutput.getDate();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(("yyyy-MM-dd"));

        sb.append(simpleDateFormat.format(timestamp));
        sb.append(",");
        sb.append(myOutput.getCellId());


        myOutput.getCountType().forEach((k,v) -> {
            sb.append(",");sb.append(k).append(",").append(String.format(Locale.ENGLISH, "%.2g", (double)v/days));
        });
        sb.append("\n");

        writer.write(sb.toString());
        writer.flush();
        writer.close();

        return sb.toString();

    }

     */

}
