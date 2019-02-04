package influxdata.flunx;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.influxdb.dto.QueryResult;

import java.util.List;

// TODO super hardcoded
public class From implements Transformation {
    @Override
    public void chain(StreamExecutionEnvironment env, DAG.Node node) {
        DataStream<Row> rowStream = env
                .addSource(new InfluxDBSource())
                .flatMap(new FlatMapFunction<QueryResult.Result, Row>() {
                    @Override
                    public void flatMap(QueryResult.Result result, Collector<Row> collector) throws Exception {
                        if (result.getSeries() == null) {
                            return;
                        }

                        for (QueryResult.Series series : result.getSeries()) {
                            List<String> columns = series.getColumns();
                            for (List<Object> values : series.getValues()) {
                                Row row = new Row(columns, values);
                                collector.collect(row);
                            }
                        }
                    }
                });
        node.setOutputStream(rowStream);
    }
}
