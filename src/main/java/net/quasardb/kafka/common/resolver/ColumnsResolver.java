package net.quasardb.kafka.common.resolver;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnsResolver extends Resolver<String> {

    private static final Logger log = LoggerFactory.getLogger(ColumnsResolver.class);
    private String[] columnNames;
    private String delim;

    public ColumnsResolver(List<String> columnNames) {
        log.debug("Initializing multi-column resolver for columns: " + columnNames.toString());
        this.columnNames = columnNames.toArray(new String[columnNames.size()]);
        this.delim = null;
    }

    public ColumnsResolver(List<String> columnNames, String delim) {
        log.debug("Initializing multi-column resolver for columns: " + columnNames.toString() + " with delimiter: " + delim);
        this.columnNames = columnNames.toArray(new String[columnNames.size()]);
        this.delim = delim;
    }

    @Override
    public String resolve(SinkRecord record) throws DataException {
        Schema schema = record.valueSchema();
        Object data = record.value();

        String[] values;

        // AVRO or JSON with schema
        if (schema != null && data instanceof Struct) {
            values = resolve((Struct)data);
        } else if (data instanceof Map) {
            values = resolve((Map)data);
        } else {
            throw new DataException("record is not Avro schema nor structured json, cannot look up column: " + data.toString());
        }

        if (this.delim != null) {
            return String.join(this.delim, values);
        } else {
            return String.join("", values);
        }
    }

    private String[] resolve(Struct data) throws DataException {
        String[] values = new String[this.columnNames.length];

        for (int i = 0; i < this.columnNames.length; ++i) {
            Object value = data.get(this.columnNames[i]);
            if (value == null) {
                throw new DataException("table column '" + this.columnNames[i] + "' not found, cannot resolve: " + data.toString());
            }

            values[i] = (String)value;
        }

        return values;
    }

    private String[] resolve(Map data) throws DataException {
        String[] values = new String[this.columnNames.length];

        for (int i = 0; i < this.columnNames.length; ++i) {
            Object value = data.get(this.columnNames[i]);
            if (value == null) {
                throw new DataException("table column '" + this.columnNames[i] + "' not found, cannot resolve: " + data.toString());
            }

            values[i] = (String)value;
        }

        return values;
    }
}
