package influxdata.flunx;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Instant;

// does not evaluate the user-defined function and uses the index as a value
public class FromGen implements Transformation {
    @Override
    public void chain(StreamExecutionEnvironment env, DAG.Node node) throws Exception {
        long start = Instant.parse(node.getSpec().get("start").asText()).toEpochMilli();
        long stop = Instant.parse(node.getSpec().get("stop").asText()).toEpochMilli();
        int count = Integer.parseInt(node.getSpec().get("count").asText());
        long delta = (stop - start) / count;

        if (delta <= 0) {
            throw new Exception("start >= stop");
        }

        DataStream<Row> ds = env.addSource(new SourceFunction<Row>() {
            @Override
            public void run(SourceContext<Row> sourceContext) throws Exception {
                long curr = start;
                for (int i = 0; i < count; i++) {
                    Row r = new Row();
                    r.addValue("_start", Instant.ofEpochMilli(start).toString());
                    r.addValue("_stop", Instant.ofEpochMilli(stop).toString());
                    r.addValue("_time", Instant.ofEpochMilli(curr + delta).toString());
                    r.addValue("_value", i);
                    curr += delta;
                    sourceContext.collect(r);
                }
            }

            @Override
            public void cancel() {
                // does nothing
            }
        });
        node.setOutputStream(ds);
    }
}
