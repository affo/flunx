package influxdata.flunx;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Yield implements Transformation {
    @Override
    public void chain(StreamExecutionEnvironment env, DAG.Node node) throws Exception {
        node.getInputStream().toVanilla().print();
    }
}
