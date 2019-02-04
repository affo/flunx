package influxdata.flunx;

import java.util.ArrayList;
import java.util.List;

public class Row {
    List<String> columns;
    List<Object> values;

    public Row() {
        columns = new ArrayList<>();
        values = new ArrayList<>();
    }

    public Row(List<String> cols, List<Object> vals) {
        columns = cols;
        values = vals;
    }

    public void addValue(String column, Object value) {
        columns.add(column);
        values.add(value);
    }

    public void setValue(String column, Object value) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(column)) {
                values.set(i, value);
                break;
            }
        }
    }

    public Object get(String column) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).equals(column)) {
                return values.get(i);
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return values.toString();
    }
}
