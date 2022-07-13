package queries.query1;

import utilities.Configurations;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ResultQuery1 {

    private Integer sensorID;
    private Double avg_temperature;
    private Long count;
    private Date timestamp;

    public ResultQuery1(Integer sensorID, Double temperature, Long occurrences) {
        this.sensorID = sensorID;
        this.avg_temperature = temperature;
        this.count = occurrences;

    }

    @Override
    public String toString() {
        return "ResultQuery1{" +
                "sensorID=" + sensorID +
                ", avg_temperature=" + avg_temperature +
                ", count=" + count +
                ", timestamp=" + timestamp +
                '}';
    }

    public Integer getSensorID() {
        return sensorID;
    }

    public void setSensorID(Integer sensorID) {
        this.sensorID = sensorID;
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

    public static String writeQuery1Result(ResultQuery1 myOutput, String evenTime) throws FileNotFoundException {
        //ts, sensor_id, count, avg_temperature



        StringBuilder sb = new StringBuilder();
        Date timestamp = myOutput.getTimestamp();
        //SimpleDateFormat simpleDateFormat = new SimpleDateFormat(("yyyy-MM-dd"));
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        sb.append(simpleDateFormat.format(timestamp));
        sb.append(",");
        sb.append(myOutput.getSensorID());
        sb.append(",");
        sb.append(myOutput.getCount());
        sb.append(",");
        sb.append(myOutput.getAvg_temperature());
        //sb.append("\n");



        return sb.toString();

    }


    public static String writeQuery1Result_2(ResultQuery1 myOutput, String evenTime) throws FileNotFoundException {
        //ts, sensor_id, count, avg_temperature

        String outputPath = "./Results/"+ Configurations.datasetPath+"_"+evenTime+"_QUERY1.csv";
        System.out.println("outputPath: "+outputPath);
        PrintWriter writer = new PrintWriter(new FileOutputStream(outputPath, true));

        StringBuilder sb = new StringBuilder();
        Date timestamp = myOutput.getTimestamp();
        //SimpleDateFormat simpleDateFormat = new SimpleDateFormat(("yyyy-MM-dd"));
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        sb.append(simpleDateFormat.format(timestamp));
        sb.append(",");
        sb.append(myOutput.getSensorID());
        sb.append(",");
        sb.append(myOutput.getCount());
        sb.append(",");
        sb.append(myOutput.getAvg_temperature());
        sb.append("\n");

        writer.write(sb.toString());
        writer.flush();
        writer.close();

        return sb.toString();

    }





}
