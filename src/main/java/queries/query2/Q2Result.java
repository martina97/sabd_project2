package queries.query2;

import utilities.Configurations;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Q2Result {
    //private HashMap<Long,Double> map;

    private Long location;
    private Double avg_temperature;
    private Long count;
    private Date timestamp;

    public Q2Result(Long location, Double temperature, Long occurrences) {
        this.location = location;
        this.avg_temperature = temperature;
        this.count = occurrences;

    }

    public static String writeQuery2Result(String myOutput, String evenTime) throws FileNotFoundException {

        String outputPath = "./Results/"+ Configurations.datasetPath+"_"+evenTime+"_QUERY2.csv";
        System.out.println("outputPath: "+outputPath);
        System.out.println("SONO IN WRITE QUERY2 ------ \n line === " + myOutput);
        String[] values = myOutput.split(";");

        PrintWriter writer = new PrintWriter(new FileOutputStream(outputPath, true));

        StringBuilder sb = new StringBuilder();
        String timestamp = values[0];
        //SimpleDateFormat simpleDateFormat = new SimpleDateFormat(("yyyy-MM-dd"));
        //SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        //myOutput = Tue May 31 00:00:00 CEST 2022,226.5=[7483],222.90000000000092=[6179],65.188068181818=[53098],29.67381679389314=[61144],28.210638297872375=[10762],-136.67999999999992=[4938],-134.67999999999938=[6268],12.400956022944545=[51487],12.638374291115301=[55953],12.668786127167623=[48128],7.761045651216123,0.1288486171761281
        sb.append(timestamp);
        sb.append(",");
        for (int i = 1; i<11;i++) {
            String[] elem = values[i].split("=");
            System.out.println("values[i] === " + values[i]);
            String location;
            if(elem[1].contains(",")) {
                //ho piu di una localita con quella temperatura, le metto entrambe!
                location = elem[1];
            }
            else {
                location = elem[1].replace("[","").replace("]","");
            }
            String temp = elem[0];
            sb.append(location);
            sb.append(",");
            sb.append(temp);
            if (i!=10) {
                sb.append(",");
            }
            else {
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
        return "ResultQuery1{" +
                "location=" + location +
                ", avg_temperature=" + avg_temperature +
                ", count=" + count +
                ", timestamp=" + timestamp +
                '}';
    }

    public Long getLocation() {
        return location;
    }

    public void setLocation(Long location) {
        this.location = location;
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
