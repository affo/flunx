package influxdata.flunx;

import java.util.ArrayList;
import java.util.List;

public class Row {
    List<String> cols;
    List<Object> vals;
    List<String> tags;

    public Row(List<String> cols, List<Object> vals) {
        this.cols = cols;
        this.vals = vals;
        this.tags = new ArrayList<>();
    }
}
