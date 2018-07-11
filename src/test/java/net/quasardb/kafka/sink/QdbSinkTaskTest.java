package net.quasardb.kafka.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.nio.ByteBuffer;
import java.io.IOException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.apache.kafka.connect.errors.DataException;

import net.quasardb.qdb.Session;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Row;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.Tables;

import net.quasardb.kafka.common.ConnectorUtils;
import net.quasardb.kafka.common.TestUtils;

public class QdbSinkTaskTest {

    private static final int    NUM_TASKS   = 10;
    private static final int    NUM_TABLES  = 4;
    private static final int    NUM_ROWS    = 1000;
    private static Value.Type[] VALUE_TYPES = { Value.Type.INT64,
                                                Value.Type.DOUBLE,
                                                Value.Type.BLOB };

    private Session             session;
    private QdbSinkTask         task;
    private Map<String, String> props;

    private Column[][]          columns;
    private Row[][]             rows;
    private Table[]             tables;

    /**
     * Kafka representation of quasardb columns. Maps 1:1 with the fields and
     * field types.
     */
    private Schema[]            schemas;

    /**
     * Kafka representation of quasardb rows. Maps 1:1 with the rows.
     */
    private SinkRecord[][]      records;


    private static SinkRecord rowToRecord(String topic,
                                          Integer partition,
                                          Schema schema,
                                          Row row) {
        return rowToRecord(topic, partition, schema, row.getValues());
    }

    private static SinkRecord rowToRecord(String topic,
                                          Integer partition,
                                          Schema schema,
                                          Value[] row) {
        Struct value = new Struct(schema);

        Field[] fields = schema.fields().toArray(new Field[schema.fields().size()]);

        // In these tests, we're using exactly one kafka schema field for every
        // field in our rows. These schemas are always the same for all rows, and
        // we're not testing ommitted fields.
        assertEquals(fields.length, row.length);

        for(int i = 0; i < fields.length; ++i) {
            switch (row[i].getType()) {
            case INT64:
                value.put(fields[i], row[i].getInt64());
                break;
            case DOUBLE:
                value.put(fields[i], row[i].getDouble());
                break;
            case BLOB:
                ByteBuffer bb = row[i].getBlob();
                int size = bb.capacity();
                byte[] buffer = new byte[size];
                bb.get(buffer, 0, size);
                bb.rewind();
                value.put(fields[i], buffer);
                break;
            default:
                throw new DataException("row field type not supported: " + value.toString());
            }
        }

        return new SinkRecord(topic, partition, null, null, schema, value, -1);
    }

    private static Schema columnsToSchema(Column[] columns) {
        SchemaBuilder builder = SchemaBuilder.struct();

        for (Column c : columns) {
            switch (c.getType()) {
            case INT64:
                builder.field(c.getName(), SchemaBuilder.int64());
                break;
            case DOUBLE:
                builder.field(c.getName(), SchemaBuilder.float64());
                break;
            case BLOB:
                builder.field(c.getName(), SchemaBuilder.bytes());
                break;
            default:
                throw new DataException("column field type not supported: " + c.toString());
            }
        }

        return builder.build();
    }

    @BeforeEach
    public void setup() throws IOException {
        this.session = TestUtils.createSession();

        this.columns = new Column[NUM_TABLES][];
        this.rows    = new Row[NUM_TABLES][];
        this.tables  = new Table[NUM_TABLES];
        this.schemas = new Schema[NUM_TABLES];
        this.records = new SinkRecord[NUM_TABLES][];

        for (int i = 0; i < NUM_TABLES; ++i) {

            // Generate a column of each value type
            this.columns[i] = Arrays.stream(VALUE_TYPES)
                .map((type) -> {
                        return TestUtils.generateTableColumn(type);
                    })
                .toArray(Column[]::new);
            this.rows[i] = TestUtils.generateTableRows(this.columns[i], NUM_ROWS);
            this.tables[i] = TestUtils.createTable(this.session, this.columns[i]);

            // Calculate/determine Kafka Connect representations of the schemas
            this.schemas[i] = columnsToSchema(this.columns[i]);

            final Schema schema = this.schemas[i];
            final String topic = this.tables[i].getName();

            this.records[i] = Arrays.stream(this.rows[i])
                .map((row) -> {
                        return rowToRecord(topic, 0, schema, row);
                    })
                .toArray(SinkRecord[]::new);
        }

        this.task = new QdbSinkTask();
        this.props = new HashMap<>();

        String topicMap = Arrays.stream(this.tables)
            .map((table) -> {
                    // Here we assume kafka topic id == qdb table id
                    return table.getName() + "=" + table.getName();
                })
            .collect(Collectors.joining(","));

        this.props.put(ConnectorUtils.CLUSTER_URI_CONFIG, "qdb://127.0.0.1:28360");
        this.props.put(ConnectorUtils.TABLES_CONFIG, topicMap);
    }

    /**
     * Tests that an exception is thrown when the schema of the value is not a struct.
     */
    @ParameterizedTest
    @MethodSource("randomSchemaWithValue")
    public void testPutValuePrimitives(Schema schema, Object value) {
        this.task.start(this.props);

        List<SinkRecord> records = new ArrayList<SinkRecord>();
        records.add(new SinkRecord(null, -1, null, null, schema, value, -1));

        if (schema != null && schema.type() == Schema.Type.STRUCT) {
            this.task.put(records);
        } else {
            assertThrows(DataException.class, () -> this.task.put(records));
        }
    }

    static Stream<Arguments> randomSchemaWithValue() {
        return Stream.of(Arguments.of(null, null),
                         Arguments.of(SchemaBuilder.int8(),    (byte)8),
                         Arguments.of(SchemaBuilder.int16(),   (short)16),
                         Arguments.of(SchemaBuilder.int32(),   (int)32),
                         Arguments.of(SchemaBuilder.int64(),   (long)64),
                         Arguments.of(SchemaBuilder.float32(), (float)32.0),
                         Arguments.of(SchemaBuilder.float64(), (double)64.0),
                         Arguments.of(SchemaBuilder.bool(),    true),
                         Arguments.of(SchemaBuilder.string(), "hi, dave"));
    }
}
