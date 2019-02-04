package influxdata.flunx;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GroupBy implements Transformation {
    @Override
    public void chain(StreamExecutionEnvironment env, DAG.Node node) {
        Set<String> cols = new HashSet<>();

        for (JsonNode c : node.getSpec().get("columns")) {
            cols.add(c.asText());
        }

        DAG.Stream is = node.getInputStream();

        KeyedStream<Row, String> ks = is
                .toVanilla() // must be a vanilla data stream
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row row) throws Exception {
                        int count = 0;
                        List<String> tags = new ArrayList<>();

                        for (String label : row.columns) {
                            if (cols.contains(label)) {
                                tags.add(row.values.get(count).toString());
                            }
                            count++;
                        }

                        return String.join("-", tags);
                    }
                });
        node.setOutputStream(ks);
    }
}
