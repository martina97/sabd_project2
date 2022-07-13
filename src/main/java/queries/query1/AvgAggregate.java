package queries.query1;

import org.apache.flink.api.common.functions.AggregateFunction;
import utilities.Sensor;

public class AvgAggregate implements AggregateFunction<Sensor, AccumulatorQuery1, ResultQuery1> {

    @Override
    public AccumulatorQuery1 createAccumulator() {
        //viene chiamato 1 volta per window (la prima volta che c'è un record di quella window)

        //istant now
        //get result faccio diff
        return new AccumulatorQuery1();
    }

    @Override
    public AccumulatorQuery1 add(Sensor values, AccumulatorQuery1 acc) {
        // per ogni record
        //System.out.println("values in aggregate == " + values);
        acc.sum += values.getTemperature();
        acc.count++;
        acc.sensor_id = values.getSensor_id();
        //acc.last_timestamp = Timestamp.valueOf(values.getTimestamp());
        return acc;
    }

    @Override
    public AccumulatorQuery1 merge(AccumulatorQuery1 a, AccumulatorQuery1 b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public ResultQuery1 getResult(AccumulatorQuery1 acc) {

        // alla fine quando viene chiusa la finestra
        double mean = acc.sum / (double) acc.count;
        //String res = "sensor_id == " + acc.sensor_id + ", mean = " + mean + ", count = " + acc.count;
        ResultQuery1 res = new ResultQuery1((int) acc.sensor_id, mean, acc.count);

        return res;
    }
}