package influxdata.flunx;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// TODO super hardcoded
public class To implements Transformation {
    @Override
    public void chain(StreamExecutionEnvironment env, DAG.Node node) throws Exception {
        DataStream<Row> ds = node.getInputStream().toVanilla();
        ds.addSink(new InfluxDBSink());
    }
}
