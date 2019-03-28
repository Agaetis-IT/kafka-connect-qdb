package net.quasardb.kafka.common.resolver;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

public abstract class ColumnResolver<T> extends Resolver<T> {

    protected String columnName;

    public ColumnResolver(QdbSinkConfig config, String columnName) {
        super(config);
        this.columnName = columnName;

        log.debug("Initializing column table resolver");
    }


    @Override
    public T resolve(SinkRecord record) throws DataException {
        Schema schema = record.valueSchema();
        Object data = record.value();

        T value;
        // AVRO or JSON with schema
        if (schema != null && data instanceof Struct) {
            value = resolve((Struct) data);
        } else if (data instanceof Map) {
            value = resolve((Map) data);
        } else {
            throw new DataException("record is not Avro schema nor structured json, cannot look up column: " + data.toString());
        }

        log.debug("T.resolve, result = {}", value);

        return value;
    }

    private T resolve(Struct data) throws DataException {
        return (T) data.get(this.columnName);
    }

    private T resolve(Map data) throws DataException {
        final Object value = data.get(this.columnName);
        if (value == null) {
            throw new DataException("table column '" + this.columnName + "' not found, cannot resolve: " + columnName);
        }
        return (T) value;
    }
}
