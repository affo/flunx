package influxdata.flunx;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

public class InfluxDBSource extends RichSourceFunction<QueryResult.Result> {
    public static final int PERIOD = 3000;
    public static final String INFLUXDB_DATABASE = "kube-infra";
    public static final String INFLUXDB_RP = "monthly";
    public static final String INFLUXDB_MEASUREMENT = "nginx";
    public static final String INFLUXDB_QUERY = "SELECT * FROM \"%s\".\"%s\".\"%s\" WHERE time > now() - %dms";

    private transient InfluxDB influxDB;
    private String query = String.format(INFLUXDB_QUERY, INFLUXDB_DATABASE, INFLUXDB_RP, INFLUXDB_MEASUREMENT, PERIOD);
    private volatile boolean stop;

    @Override
    public void open(Configuration parameters) throws Exception {
        influxDB = InfluxDBFactory.connect(
                FluxJob.INFLUXDB_URL, FluxJob.INFLUXDB_USERNAME, FluxJob.INFLUXDB_PASSWORD);
        influxDB.setDatabase(INFLUXDB_DATABASE);
        influxDB.enableBatch(BatchOptions.DEFAULTS);
    }

    @Override
    public void run(SourceContext<QueryResult.Result> sourceContext) throws Exception {
        while (!stop) {
            Query query = new Query(this.query, INFLUXDB_DATABASE);
            QueryResult result = influxDB.query(query);
            for (QueryResult.Result r : result.getResults()) {
                sourceContext.collect(r);
            }

            Thread.sleep(PERIOD);
        }
    }

    @Override
    public void cancel() {
        this.stop = true;
    }
}
