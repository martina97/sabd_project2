package queries.query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utilities.Sensor;

public class Q1Aggregate implements AggregateFunction<Sensor, Q1Accumulator, Q1Result> {

    @Override
    public Q1Accumulator createAccumulator() {
        return new Q1Accumulator();
    }

    @Override
    public Q1Accumulator add(Sensor values, Q1Accumulator acc) {
        // per ogni record
        //System.out.println("values in aggregate == " + values);
        acc.sum += values.getTemperature();
        acc.count++;
        acc.sensor_id = values.getSensor_id();
        return acc;
    }

    @Override
    public Q1Accumulator merge(Q1Accumulator a, Q1Accumulator b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public Q1Result getResult(Q1Accumulator acc) {

        // alla fine quando viene chiusa la finestra
        double mean = acc.sum / (double) acc.count;
        //String res = "sensor_id == " + acc.sensor_id + ", mean = " + mean + ", count = " + acc.count;
        Q1Result res = new Q1Result((int) acc.sensor_id, mean, acc.count);

        return res;
    }
}