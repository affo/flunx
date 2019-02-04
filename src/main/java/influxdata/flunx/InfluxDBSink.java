package influxdata.flunx;

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
public class InfluxDBSink extends RichSinkFunction<Row> {
    private transient InfluxDB influxDB;
    private String dbName;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        influxDB = InfluxDBFactory.connect(
                FluxJob.INFLUXDB_URL, FluxJob.INFLUXDB_USERNAME, FluxJob.INFLUXDB_PASSWORD);
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
    public void invoke(Row row, Context context) throws Exception {
        influxDB.write(Point.measurement("nginx_request_count")
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("_value", (int) row.get("count"))
                .tag("path", row.get("key").toString())
                .build());
    }
}
