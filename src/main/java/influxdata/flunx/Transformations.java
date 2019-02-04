package influxdata.flunx;

import java.util.HashMap;
import java.util.Map;

public class Transformations {
    private static Map<String, Transformation> txs;

    static {
        txs = new HashMap<>();
        txs.put("from", new From());
        txs.put("fromGenerator", new FromGen());
        txs.put("group", new GroupBy());
        txs.put("count", new Count());
        txs.put("window", new Window());
        txs.put("yield", new Yield());
        txs.put("to", new To());
    }

    public static Transformation get(String kind) {
        return txs.get(kind);
    }
}
