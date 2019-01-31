package influxdata.flunx;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

public class GroupBy implements Transformation<Row, Row> {
    @Override
    public DataStream<Row> apply(DAG.Node node, DataStream<Row> in) {
        String col = "";

        // the last column
        for (JsonNode c : node.spec.get("spec").get("columns")) {
            col = c.asText();
        }

        String finalCol = col;
        return in.keyBy(new KeySelector<Row, String>() {
            @Override
            public String getKey(Row row) throws Exception {
                int idx = -1;
                int count = 0;

                for (String label : row.cols) {
                    if (label.equals(finalCol)) {
                        idx = count;
                        break;
                    }
                    count++;
                }
                String tag = row.vals.get(idx).toString();
                row.tags.add(tag);
                return tag;
            }
        });
    }
}
