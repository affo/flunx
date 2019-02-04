package influxdata.flunx;

import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Window implements Transformation {
    @Override
    public void chain(StreamExecutionEnvironment env, DAG.Node node) {
        // TODO account for different window types
        String every = node.getSpec().get("every").asText();

        // TODO parse duration properly
        StringBuilder sb = new StringBuilder();
        for (char c : every.toCharArray()) {
            if (Character.isDigit(c)) {
                sb.append(c);
            } else {
                break;
            }
        }
        long secs = Long.parseLong(sb.toString());

        DAG.Stream inputStream = node.getInputStreams().get(0);

        WindowedStream<Row, String, TimeWindow> ws = inputStream
                .toKeyed() // must be partitioned
                .timeWindow(Time.seconds(secs));
        node.setOutputStream(ws);
    }
}
