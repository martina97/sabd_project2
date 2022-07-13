package queries.query3;

import utilities.Cell;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public class Q3Accumulator implements Serializable {

    // creo mappa dove key = location, value = temp media
  //  private HashMap<Long, Double> rankMap;
    public long count;
    public double sum;
    public Cell cell;
    public ArrayList<Double> listTemperatures = new ArrayList<>();
    public Queue<Double> minHeap = new PriorityQueue<>();
    Queue<Double> maxHeap = new PriorityQueue<>(Comparator.reverseOrder());
}
