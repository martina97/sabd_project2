package queries.query3;

import org.apache.flink.api.common.functions.AggregateFunction;
import utilities.Sensor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Queue;

public class Q3Aggregate implements AggregateFunction<Sensor, Q3Accumulator, Q3Result> {
    @Override
    public Q3Accumulator createAccumulator() {
        //viene chiamato 1 volta per window (la prima volta che c'è un record di quella window)

        //istant now
        //get result faccio diff
        return new Q3Accumulator();
    }

    @Override
    public Q3Accumulator add(Sensor values, Q3Accumulator acc) {
        // per ogni record
        //System.out.println("values in aggregate == " + values);

        acc.sum += values.getTemperature();
        acc.count++;
        acc.cell = values.getCell();
        //System.out.println("cella ==== " + values.getCell().getIdCell() + " temp == " +values.getTemperature());
        acc.listTemperatures.add(values.getTemperature());

        if (!acc.minHeap.isEmpty() && values.getTemperature() < acc.minHeap.peek()) {
            acc.maxHeap.offer(values.getTemperature());
            if (acc.maxHeap.size() > acc.minHeap.size() + 1) {
                acc.minHeap.offer(acc.maxHeap.poll());
            }
        } else {
            acc.minHeap.offer(values.getTemperature());
            if (acc.minHeap.size() > acc.maxHeap.size() + 1) {
                acc.maxHeap.offer(acc.minHeap.poll());
            }
        }




        //System.out.println("in add listTemperatures === " + acc.listTemperatures);
        //acc.last_timestamp = Timestamp.valueOf(values.getTimestamp());
        return acc;
    }

    @Override
    public Q3Accumulator merge(Q3Accumulator a, Q3Accumulator b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public Q3Result getResult(Q3Accumulator acc) {

        // alla fine quando viene chiusa la finestra
        double mean = acc.sum / (double) acc.count;
       // System.out.println("list temperatures === " + acc.listTemperatures);
        double median = calculateMedian(acc.listTemperatures);
        double medianHeap= getMedian(acc.maxHeap,acc.minHeap);
        //System.out.println("median === " + median);
        //String res = "sensor_id == " + acc.sensor_id + ", mean = " + mean + ", count = " + acc.count;
        Q3Result res = new Q3Result(acc.cell, mean, median,medianHeap,acc.count);

        return res;
    }

    private double getMedian(Queue<Double> maxHeap, Queue<Double> minHeap) {
        double median;
        if (minHeap.size() < maxHeap.size()) {
            median = maxHeap.peek();
        } else if (minHeap.size() > maxHeap.size()) {
            median = minHeap.peek();
        } else {
            median = (minHeap.peek() + maxHeap.peek()) / 2;
        }
        return median;
    }

    private double calculateMedian(ArrayList<Double> listTemperatures) {

        Collections.sort(listTemperatures);
        double median;
        if (listTemperatures.size() % 2 == 0)
            median = ((double) listTemperatures.get(listTemperatures.size() / 2) + (double) listTemperatures.get(listTemperatures.size() / 2 - 1))/2;
        else
            median = (double) listTemperatures.get(listTemperatures.size() / 2);
        return median;
    }
}
