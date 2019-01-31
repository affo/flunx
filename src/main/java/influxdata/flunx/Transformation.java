package influxdata.flunx;

import org.apache.flink.streaming.api.datastream.DataStream;

public interface Transformation<I, O> {
    DataStream<O> apply(DAG.Node node, DataStream<I> in);
}
