package utilities;


import queries.query3.Query3;
import java.util.ArrayList;

public class Sensor {

    Integer sensor_id;
    String sensor_type;
    Long location;
    Double lat;
    Double lon;
    String timestamp;
    Double pressure;
    Double altitude;
    Double pressure_sealevel;
    Double temperature;
    Cell cell;
    public Cell getCell() {
        return cell;
    }

    public void setCell() {

        this.cell = null; //lo metto null così se ho valori di lat e lon che stanno fuori la griglia, ho settato null
        ArrayList<utilities.Cell> grid = Query3.createGrid();

        if (lat!=null && lon != null) {
            for (Cell cell : grid) {
                // System.out.println("cell ---> " + cell);
                if ( cell.getLat2()<lat && lat <cell.getLat1() && cell.getLon1()<lon && lon < cell.getLon2()) {
                    //System.out.println("la cella per il sensore " + sensor_id + " ==  " + cell.getIdCell());
                    this.cell = cell;
                    break;
                }
            }
        }
        else {
           // System.out.println("CELLA NON VALIDA");
            this.cell = null;
        }

    }



    public Integer getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(Integer occurrences) {
        this.occurrences = occurrences;
    }

    Integer occurrences;

    public Sensor(Integer sensor_id, String sensor_type, Long location, Double lat, Double lon, String timestamp, Double pressure, Double temperature) {
        this.sensor_id = sensor_id;
        this.sensor_type = sensor_type;
        this.location = location;
        this.lat = lat;
        this.lon = lon;
        this.timestamp = timestamp;
        this.pressure = pressure;
        this.temperature = temperature;

    }



    public Integer getSensor_id() {
        return sensor_id;
    }

    public void setSensor_id(Integer sensor_id) {
        this.sensor_id = sensor_id;
    }

    public String getSensor_type() {
        return sensor_type;
    }

    public void setSensor_type(String sensor_type) {
        this.sensor_type = sensor_type;
    }

    public Long getLocation() {
        return location;
    }

    public void setLocation(Long location) {
        this.location = location;
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public Double getLon() {
        return lon;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Double getPressure() {
        return pressure;
    }

    public void setPressure(Double pressure) {
        this.pressure = pressure;
    }

    public Double getAltitude() {
        return altitude;
    }

    public void setAltitude(Double altitude) {
        this.altitude = altitude;
    }

    public Double getPressure_sealevel() {
        return pressure_sealevel;
    }

    public void setPressure_sealevel(Double pressure_sealevel) {
        this.pressure_sealevel = pressure_sealevel;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }


    @Override
    public String toString() {
        return "Sensor{" +
                "sensor_id=" + sensor_id +
                ", sensor_type='" + sensor_type + '\'' +
                ", location=" + location +
                ", lat=" + lat +
                ", lon=" + lon +
                ", timestamp='" + timestamp + '\'' +
                ", pressure=" + pressure +
                ", altitude=" + altitude +
                ", pressure_sealevel=" + pressure_sealevel +
                ", temperature=" + temperature +
                ", cell=" + cell +
                ", occurrences=" + occurrences +
                '}';
    }

    public static Double checkOutliersLatLon(String value) {

        /*
         * ci sono outlier per la latitudine e longitudine, ad esempio 5.093.518.312
         * visto che per ora non mi serve avere latitudine e longitudine, metto null
         */
        Double output;
        try {
            output = Double.parseDouble(value);
        } catch ( NumberFormatException e ) {
            System.out.println("sono in catch");
            output = null;
        }
        //System.out.println("exception == " + output);
        return output;
    }

}
