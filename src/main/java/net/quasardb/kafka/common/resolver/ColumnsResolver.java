package net.quasardb.kafka.common.resolver;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;
import java.util.Map;

public class ColumnsResolver extends Resolver<String> {

    private final String[] columnNames;
    private final String delimiter;

    public ColumnsResolver(QdbSinkConfig config, List<String> columnNames, String delimiter) {
        super(config);
        this.columnNames = columnNames.toArray(new String[columnNames.size()]);
        this.delimiter = delimiter;
        log.debug("Initializing multi-column resolver for columns: {} with delimiter: {}", columnNames, delimiter);
    }

    @Override
    public String resolve(SinkRecord record) throws DataException {
        Schema schema = record.valueSchema();
        Object data = record.value();

        String[] values;

        // AVRO or JSON with schema
        if (schema != null && data instanceof Struct) {
            values = resolve((Struct) data);
        } else if (data instanceof Map) {
            values = resolve((Map) data);
        } else {
            throw new DataException("record is not Avro schema nor structured json, cannot look up column: " + data.toString());
        }

        return String.join(this.delimiter, values);
    }

    private String[] resolve(Struct data) throws DataException {
        String[] values = new String[this.columnNames.length];

        for (int i = 0; i < this.columnNames.length; ++i) {
            Object value = data.get(this.columnNames[i]);
            values[i] = (String) value;
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

            values[i] = (String) value;
        }

        return values;
    }
}
