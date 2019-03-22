package net.quasardb.kafka.common;

import java.util.Map;
import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Value;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API for various strategies for converting a Kafka record into
 * a QuasarDB row.
 */
public class RecordConverter {

    private static final Logger log = LoggerFactory.getLogger(RecordConverter.class);

    public static Value convert(Column qdbColumn, String recordColumn, SinkRecord record) throws DataException {
        return doConvert(qdbColumn, recordColumn, record.valueSchema(), record.value());
    }

    public static Value[] convert(Column[] columns, SinkRecord record) throws DataException {
        return doConvert(columns, record.valueSchema(), record.value());
    }


    private static Value doConvert(Column qdbColumn, String recordColumn, Schema schema, Object data) throws DataException {

        // AVRO or JSON with schema
        if (schema != null && data instanceof Struct) {
            return doConvert(qdbColumn, recordColumn, (Struct)data);
        }

        // structured JSON
        if (data instanceof Map) {
            return doConvert(qdbColumn, recordColumn, (Map)data);
        }

        throw new DataException("Only schemaful converters are currently supported, input: " + data.toString());
    }

    private static Value[] doConvert(Column[] columns, Schema schema, Object data) throws DataException {

        // AVRO or JSON with schema
        if (schema != null && data instanceof Struct) {
            return doConvert(columns, (Struct)data);
        }

        // structured JSON
        if (data instanceof Map) {
            return doConvert(columns, (Map)data);
        }

        throw new DataException("Only schemaful converters are currently supported, input: " + data.toString());
    }


    private static Value doConvert(Column qdbColumn, String recordColumn, Struct data) throws DataException {
        Object value = data.get(recordColumn);
        if (value != null) {
            switch(qdbColumn.getType()) {
            case INT64:
                if (value instanceof Long) {
                    return Value.createInt64((Long)value);
                }

                log.warn("Ignoring int64 column '{}': expected Long value, got: {}", qdbColumn.getName (), value.getClass());
                return Value.createNull();
            case DOUBLE:
                if (value instanceof Double) {
                    return Value.createDouble((Double)value);
                }

                if (value instanceof Long) {
                    return Value.createDouble(((Long)value).doubleValue());
                }

                log.warn("Ignoring double column '{}': expected Double value, got: {}", qdbColumn.getName (), value.getClass());
                return Value.createNull();
            case TIMESTAMP:
                if (value instanceof Long) {
                  return Value.createTimestamp(new Timespec((Long)value));
                }

                log.warn("Ignoring timestamp column '{}': expected Long value, got: {}", qdbColumn.getName(), value.getClass());
                return Value.createNull();
            case BLOB:
                if (value instanceof byte[]) {
                    return Value.createSafeBlob((byte[])value);
                }

                log.warn("Ignoring blob column '{}': expected String value, got: {}", qdbColumn.getName (), value.getClass());
                return Value.createNull();
            }
        }

        log.warn("key not found, setting null value: {}", qdbColumn.getName());
        return Value.createNull();
    }

    private static Value[] doConvert(Column[] columns, Struct data) throws DataException {
        Value[] out = new Value[columns.length];

        for (int i = 0; i < columns.length; ++i) {
            Column c = columns[i];

            out[i] = doConvert(c, c.getName(), data);
        }

        return out;
    }

    private static Value doConvert(Column qdbColumn, String recordColumn, Map data) throws DataException {
        Object value = data.get(recordColumn);
        if (value != null) {
            switch(qdbColumn.getType()) {
            case INT64:
                if (value instanceof Long) {
                    return Value.createInt64((Long)value);
                }

                log.warn("Ignoring int64 column '{}': expected Long value, got: {}", qdbColumn.getName(), value.getClass());
                return Value.createNull();
            case DOUBLE:
                if (value instanceof Double) {
                    return Value.createDouble((Double)value);
                }

                if (value instanceof Long) {
                    return Value.createDouble(((Long)value).doubleValue());
                }

                log.warn("Ignoring double column '{}': expected Double value, got: {}", qdbColumn.getName(), value.getClass());
                return Value.createNull();
            case TIMESTAMP:
                if (value instanceof Long) {
                    return Value.createTimestamp(new Timespec((Long)value));
                }

                log.warn("Ignoring timestamp column '{}': expected Long value, got: {}", qdbColumn.getName(), value.getClass());
                return Value.createNull();
            case BLOB:
                if (value instanceof String) {
                    return Value.createSafeString((String)value);
                }

                log.warn("Ignoring blob column '{}': expected String value, got: {}", qdbColumn.getName(), value.getClass());
                return Value.createNull();
            }
        }

        log.warn("key not found, setting null value: {}", qdbColumn.getName());
        return Value.createNull();
    }


    private static Value[] doConvert(Column[] columns, Map data) throws DataException {
        Value[] out = new Value[columns.length];

        for (int i = 0; i < columns.length; ++i) {
            Column c = columns[i];

            out[i] = doConvert(c, c.getName(), data);
        }

        return out;
    }
}
