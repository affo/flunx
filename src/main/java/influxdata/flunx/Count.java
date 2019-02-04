package influxdata.flunx;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Count implements Transformation {
    @Override
    public void chain(StreamExecutionEnvironment env, DAG.Node node) {
        DAG.Stream is = node.getInputStream();
        DataStream<Row> out;
        if (is.isVanilla() || is.isKeyed()) {
            out = is
                    .toVanilla()
                    .map(new MapFunction<Row, Row>() {
                        int count;

                        @Override
                        public Row map(Row row) throws Exception {
                            // WARNING: side-effect on row
                            count++;
                            row.columns.add("count");
                            row.values.add(count);
                            return row;
                        }
                    });
        } else {
            out = is
                    .toWindowed()
                    .apply(new WindowFunction<Row, Row, String, TimeWindow>() {
                        @Override
                        public void apply(String s, TimeWindow timeWindow, Iterable<Row> content, Collector<Row> collector) throws Exception {
                            int count = 0;
                            for (Row r : content) {
                                count++;
                            }

                            Row r = new Row();
                            r.addValue("_start", timeWindow.getStart());
                            r.addValue("_stop", timeWindow.getEnd());
                            r.addValue("key", s);
                            r.addValue("count", count);
                            collector.collect(r);
                        }
                    });
        }

        node.setOutputStream(out);
    }
}
