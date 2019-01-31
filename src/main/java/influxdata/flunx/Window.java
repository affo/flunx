package influxdata.flunx;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Window implements Transformation<Row, List<Row>> {
    @Override
    public DataStream<List<Row>> apply(DAG.Node node, DataStream<Row> in) {
        String every = node.spec.get("spec").get("every").asText();
        long secs = 10; // TODO

        return ((KeyedStream<Row, String>) in)
                .timeWindow(Time.seconds(secs))
                .apply(new WindowFunction<Row, List<Row>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<Row> iterable, Collector<List<Row>> collector) throws Exception {
                        List<Row> rows = new ArrayList<>();
                        for (Row r : iterable) {
                            rows.add(r);
                        }
                        collector.collect(rows);
                    }
                });
    }
}
