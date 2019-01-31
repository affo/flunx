package influxdata.flunx;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

public class WindowCount implements Transformation<List<Row>, Tuple3<String, Integer, String>> {
    @Override
    public DataStream<Tuple3<String, Integer, String>> apply(DAG.Node node, DataStream<List<Row>> in) {
        return in.map(new MapFunction<List<Row>, Tuple3<String, Integer, String>>() {
            @Override
            public Tuple3<String, Integer, String> map(List<Row> window) throws Exception {
                return Tuple3.of("nginx_request_count", window.size(), window.get(0).tags.get(0));
            }
        });
    }
}
