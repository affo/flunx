package influxdata.flunx;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.ParseException;

public interface Transformation {
    void chain(StreamExecutionEnvironment env, DAG.Node node) throws Exception;
}
