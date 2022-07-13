package utilities;

/*
    (lat1,lon1) ------------- (lat1, lon2)
         ╎                          ╎
         ╎                          ╎
         ╎                          ╎
         ╎                          ╎
    (lat2,lon1) ------------- (lat2, lon2)

 */

public class Cell {
    Double lat1;
    Double lat2;
    Double lon1;
    Double lon2;
    String idCell;

    public Cell(Double lat1, Double lon1, Double lat2, Double lon2, String idCell) {
        this.lat1 = lat1;
        this.lat2 = lat2;
        this.lon1 = lon1;
        this.lon2 = lon2;
        this.idCell = idCell;
    }

    public Double getLat1() {
        return lat1;
    }

    public void setLat1(Double lat1) {
        this.lat1 = lat1;
    }

    public Double getLat2() {
        return lat2;
    }

    public void setLat2(Double lat2) {
        this.lat2 = lat2;
    }

    public Double getLon1() {
        return lon1;
    }

    public void setLon1(Double lon1) {
        this.lon1 = lon1;
    }

    public Double getLon2() {
        return lon2;
    }

    public void setLon2(Double lon2) {
        this.lon2 = lon2;
    }

    public String getIdCell() {
        return idCell;
    }

    public void setIdCell(String idCell) {
        this.idCell = idCell;
    }

    @Override
    /*
    public String toString1() {
        return "Cell{" +
                "lat1=" + lat1 +
                ", lat2=" + lat2 +
                ", lon1=" + lon1 +
                ", lon2=" + lon2 +
                ", idCell='" + idCell + '\'' +
                '}';
    }

     */
    public String toString() {
        return "("+lat1+","+lon1+")" +"--- ("+lat1+","+lon2+")\n"+
                "      ╎          ╎\n"+
                "("+lat2+","+lon1+")" +"--- ("+lat2+","+lon2+")\n";
    }
}
