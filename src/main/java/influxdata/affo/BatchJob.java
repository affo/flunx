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

package influxdata.affo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.InfluxDBInputFormat;
import org.apache.flink.util.Collector;
import org.influxdb.dto.QueryResult;

import java.util.List;

public class BatchJob {
    private static final String INFLUXDB_URL = "http://localhost:8086";
    private static final String INFLUXDB_USERNAME = "root";
    private static final String INFLUXDB_PASSWORD = "root";
    private static final String INFLUXDB_DATABASE = "test";
    private static final String INFLUXDB_QUERY = "SELECT * FROM \"m0\"";

    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        long count = env.createInput(InfluxDBInputFormat.create()
                .url(INFLUXDB_URL)
                .username(INFLUXDB_USERNAME)
                .password(INFLUXDB_PASSWORD)
                .database(INFLUXDB_DATABASE)
                .query(INFLUXDB_QUERY)
                .and().buildIt())
                .flatMap(new FlatMapFunction<QueryResult.Result, List<Object>>() {
                    @Override
                    public void flatMap(QueryResult.Result result, Collector<List<Object>> collector) throws Exception {
                        for (QueryResult.Series series : result.getSeries()) {
                            for (List<Object> values : series.getValues()) {
                                collector.collect(values);
                            }
                        }
                    }
                })
                .map(new MapFunction<List<Object>, Double>() {
                    @Override
                    public Double map(List<Object> values) throws Exception {
                        return (Double) values.get(values.size() - 1);
                    }
                })
                .count();

        System.out.println("The number of rows is " + count);

        // No need to execute if there is a sink
        //env.execute("Extracting from InfluxDB");
    }
}
