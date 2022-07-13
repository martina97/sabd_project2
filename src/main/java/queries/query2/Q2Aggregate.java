package queries.query2;

import org.apache.flink.api.common.functions.AggregateFunction;
import queries.query1.AccumulatorQuery1;
import queries.query1.ResultQuery1;
import utilities.Sensor;

public class Q2Aggregate implements AggregateFunction<Sensor, Q2Accumulator, Q2Result> {
    @Override
    public Q2Accumulator createAccumulator() {
        //viene chiamato 1 volta per window (la prima volta che c'è un record di quella window)

        //istant now
        //get result faccio diff
        return new Q2Accumulator();
    }

    @Override
    public Q2Accumulator add(Sensor values, Q2Accumulator acc) {
        // per ogni record
        //System.out.println("values in aggregate == " + values);
        acc.sum += values.getTemperature();
        acc.count++;
        acc.location = values.getLocation();
        //acc.last_timestamp = Timestamp.valueOf(values.getTimestamp());
        return acc;
    }

    @Override
    public Q2Accumulator merge(Q2Accumulator a, Q2Accumulator b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public Q2Result getResult(Q2Accumulator acc) {

        // alla fine quando viene chiusa la finestra
        double mean = acc.sum / (double) acc.count;
        //String res = "sensor_id == " + acc.sensor_id + ", mean = " + mean + ", count = " + acc.count;
        Q2Result res = new Q2Result(acc.location, mean, acc.count);

        return res;
    }
}
