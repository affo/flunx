/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package influxdata.flunx;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.influxdb.dto.QueryResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamingJob {
    public static final String INFLUXDB_URL = "http://influxdb.monitoring.svc.cluster.local:8086";
    public static final String INFLUXDB_USERNAME = "test";
    public static final String INFLUXDB_PASSWORD = "test";

    private static Map<String, Transformation> txs;

    static {
        txs = new HashMap<>();
        txs.put("group", new GroupBy());
        txs.put("count", new WindowCount());
        txs.put("window", new Window());
    }

    // need an output, we suppose we have only one sink.
    private static DataStream out;

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Row> rowStream = env.addSource(new InfluxDBSource())
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

        DAG dag = DAG.fromSPEC("count_spec.json");

        dag.walk(new V(rowStream));
        out.addSink(new InfluxDBSink());

        // execute program
        env.execute("Flunx");
    }

    private static class V implements DAG.Visitor {
        DataStream in;

        public V(DataStream in) {
            this.in = in;
        }

        @Override
        public DAG.Visitor Visit(DAG.Node node) {
            String kind = node.spec.get("kind").asText();
            Transformation t = txs.get(kind);

            if (t == null) {
                return new V(in);
            }

            DataStream out = t.apply(node, in);
            StreamingJob.out = out;
            return new V(out);
        }
    }
}
