package influxdata.flunx;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FluxJob {
    public static StreamExecutionEnvironment env;
    public static final String INFLUXDB_URL = "http://influxdb.monitoring.svc.cluster.local:8086";
    public static final String INFLUXDB_USERNAME = "test";
    public static final String INFLUXDB_PASSWORD = "test";

    public static void main(String[] args) throws Exception {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        String specPath = params.getRequired("spec");

        DAG dag = DAG.fromSPEC(specPath);
        dag.walk(DAG.Node::chainTransformation);

        // execute program
        env.execute("Flunx");
    }
}
