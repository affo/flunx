package influxdata.flunx;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

/**
 * Created by affo on 22/08/17.
 */
public class InfluxDBSink<T extends Tuple> extends RichSinkFunction<T> {
    private transient InfluxDB influxDB;
    private String dbName;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        influxDB = InfluxDBFactory.connect(
                StreamingJob.INFLUXDB_URL, StreamingJob.INFLUXDB_USERNAME, StreamingJob.INFLUXDB_PASSWORD);
        dbName = "streaming-job-test";
        influxDB.createDatabase(dbName);
        influxDB.setDatabase(dbName);
        influxDB.enableBatch(BatchOptions.DEFAULTS);
    }

    @Override
    public void close() throws Exception {
        super.close();
        influxDB.close();
    }

    @Override
    public void invoke(T t, Context context) throws Exception {
        synchronized (influxDB) {
            influxDB.write(Point.measurement(t.getField(0))
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("_value", (int) t.getField(1))
                    .tag("path", t.getField(2))
                    .build());
        }
    }
}
