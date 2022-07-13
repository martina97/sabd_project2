package queries.query2;


import java.io.Serializable;
import java.util.HashMap;

public class Q2Accumulator implements Serializable {

    // creo mappa dove key = location, value = temp media
  //  private HashMap<Long, Double> rankMap;
    public long count;
    public double sum;
    public long location;
}
