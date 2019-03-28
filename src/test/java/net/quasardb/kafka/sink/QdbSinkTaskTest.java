package net.quasardb.kafka.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.stream.Stream;
import java.util.stream.IntStream;
import java.io.IOException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.common.record.TimestampType;

import org.apache.kafka.connect.errors.DataException;

import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Reader;
import net.quasardb.qdb.ts.Row;
import net.quasardb.qdb.ts.TimeRange;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.Tables;

import net.quasardb.kafka.common.TestUtils;
import net.quasardb.kafka.common.Fixture;

public class QdbSinkTaskTest {

    private static final int     NUM_TASKS    = 10;
    private static Schema.Type[] SCHEMA_TYPES = { Schema.Type.STRING, Schema.Type.STRUCT };
    private static QdbSinkTask   task;

    @BeforeEach
    public void setup() {
        this.task = new QdbSinkTask();
    }

    @AfterEach
    public void teardown() {
        System.gc();
    }

    /**
     * Tests that an exception is thrown when the schema of the value is not a struct.
     */
    @ParameterizedTest
    @MethodSource("randomSchemaWithValue")
    public void testPutValuePrimitives(Fixture state, Schema schema, Object value) {
        Map<String, String> props = state.props;
        props.put(QdbSinkConfig.TABLE_FROM_TOPIC_CONFIG, "true");

        this.task.start(props);

        List<SinkRecord> records = new ArrayList<SinkRecord>();
        records.add(new SinkRecord(null, -1, null, null, schema, value, -1));

        if (schema != null && schema.type() == Schema.Type.STRUCT) {
            this.task.put(records);
        } else {
            assertThrows(DataException.class, () -> this.task.put(records));
        }
    }

    /**
     * Tests that regular rows can be inserted without problem.
     */
    @ParameterizedTest
    @MethodSource("randomRecord")
    public void testPutRow(Fixture fixture, Integer offset, Row row, SinkRecord record) {
        Map<String, String> props = fixture.props;
        props.put(QdbSinkConfig.TABLE_FROM_TOPIC_CONFIG, "true");

        this.task.start(props);
        this.task.put(Collections.singletonList(record));
        this.task.stop();
    }

    /**
     * Tests that a collection of regular rows can be inserted without problem.
     */
    @ParameterizedTest
    @MethodSource("randomRecords")
    public void testPutRows(Fixture fixture, Integer offset, Collection<SinkRecord> records) {
        Map<String, String> props = fixture.props;
        props.put(QdbSinkConfig.TABLE_FROM_TOPIC_CONFIG, "true");

        this.task.start(props);
        this.task.put(records);
        this.task.flush(new HashMap<>());

        this.task.stop();
    }

    /**
     * Tests that rows are visible after flushing.
     */
    @ParameterizedTest
    @MethodSource("randomRecord")
    public void testRowsVisibleAfterFlush(Fixture fixture,
                                          Integer offset,
                                          Row row,
                                          SinkRecord record) {
        Map<String, String> props = fixture.props;
        props.put(QdbSinkConfig.TABLE_FROM_TOPIC_CONFIG, "true");

        this.task.start(props);
        this.task.put(Collections.singletonList(record));
        this.task.flush(new HashMap<>());

        // Sleep 1 seconds, our flush interval
        try {
            Thread.sleep(1100);
        } catch (Exception e) {
            throw new Error("Unexpected exception", e);
        }

        String tableName = record.topic();

        Timespec ts = new Timespec(record.timestamp());
        TimeRange[] ranges = { new TimeRange(ts, ts.plusNanos(1)) };

        Reader reader = Table.reader(TestUtils.createSession(), tableName, ranges);
        assertEquals(true, reader.hasNext());

        Row row2 = reader.next();
        assertEquals(row, row2);
        assertEquals(false, reader.hasNext());

        this.task.stop();
    }

    /**
     * Tests that a new table can be created by a skeleton.
     */
    @ParameterizedTest
    @MethodSource("randomRecord")
    public void testAutoCreateTable(Fixture fixture,
                                    Integer offset,
                                    Row row,
                                    SinkRecord record) {
        Map<String, String> props = fixture.props;
        String newTableName = TestUtils.createUniqueAlias();

        props.put(QdbSinkConfig.TABLE_CONFIG, newTableName);
        props.put(QdbSinkConfig.TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG, Fixture.SKELETON_COLUMN_ID);
        props.put(QdbSinkConfig.TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG, "_skeleton");

        this.task.start(props);
        this.task.put(Collections.singletonList(record));
        this.task.flush(new HashMap());

        // Sleep 1 seconds, our flush interval
        try {
            Thread.sleep(1100);
        } catch (Exception e) {
            throw new Error("Unexpected exception", e);
        }

        Timespec ts = new Timespec(record.timestamp());
        TimeRange[] ranges = { new TimeRange(ts, ts.plusNanos(1)) };

        Reader reader = Table.reader(TestUtils.createSession(), newTableName, ranges);
        assertEquals(true, reader.hasNext());

        Row row2 = reader.next();
        assertEquals(row, row2);
        assertEquals(false, reader.hasNext());

        this.task.stop();
    }


    /**
     * Tests that a new table's tags can be set automatically.
     */
    @ParameterizedTest
    @MethodSource("randomRecord")
    public void testAutoCreateTableWithStaticTags(Fixture fixture,
                                                  Integer offset,
                                                  Row row,
                                                  SinkRecord record) {
        Map<String, String> props = fixture.props;
        String newTableName = TestUtils.createUniqueAlias();
        List<String> newTableTags = Arrays.asList(TestUtils.createUniqueAlias(),
                                                  TestUtils.createUniqueAlias());

        props.put(QdbSinkConfig.TABLE_CONFIG, newTableName);
        props.put(QdbSinkConfig.TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG, Fixture.SKELETON_COLUMN_ID);
        props.put(QdbSinkConfig.TABLE_AUTOCREATE_TAGS_CONFIG, String.join(",", newTableTags));

        this.task.start(props);
        this.task.put(Collections.singletonList(record));
        this.task.flush(new HashMap());

        for (String tag : newTableTags) {
            Tables tables = Tables.ofTag(TestUtils.createSession(), tag);

            assertEquals(true, tables.hasTableWithName(newTableName));
        }

        this.task.stop();
    }


    /**
     * Tests that a new table's tags can be set automatically.
     */
    @ParameterizedTest
    @MethodSource("randomRecord")
    public void testAutoCreateTableWithColumnTags(Fixture fixture,
                                                  Integer offset,
                                                  Row row,
                                                  SinkRecord record) {
        Map<String, String> props = fixture.props;
        String newTableName = TestUtils.createUniqueAlias();

        props.put(QdbSinkConfig.TABLE_CONFIG, newTableName);
        props.put(QdbSinkConfig.TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG, Fixture.SKELETON_COLUMN_ID);
        props.put(QdbSinkConfig.TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG, Fixture.TAGS_COLUMN_ID);

        this.task.start(props);
        this.task.put(Collections.singletonList(record));
        this.task.flush(new HashMap());

        // Figure out which tags are expected
        String[] newTableTags = fixture.tags[offset];

        for (String tag : newTableTags) {
            Tables tables = Tables.ofTag(TestUtils.createSession(), tag);

            assertEquals(true, tables.hasTableWithName(newTableName));
        }

        this.task.stop();
    }

    /**
     * Tests that a new table's tags can be set automatically.
     */
    @ParameterizedTest
    @MethodSource("randomRecord")
    public void testResolveTableFromColumn(Fixture fixture,
                                           Integer offset,
                                           Row row,
                                           SinkRecord record) {
        Map<String, String> props = fixture.props;

        props.put(QdbSinkConfig.TABLE_FROM_COLUMN_CONFIG, Fixture.TABLE_COLUMN_ID);
        props.put(QdbSinkConfig.TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG, Fixture.SKELETON_COLUMN_ID);

        this.task.start(props);
        this.task.put(Collections.singletonList(record));
        this.task.flush(new HashMap());

        // Sleep 1 seconds, our flush interval
        try {
            Thread.sleep(1100);
        } catch (Exception e) {
            throw new Error("Unexpected exception", e);
        }

        String tableFromColumnName = fixture.tableFromColumnName[offset];

        Timespec ts = new Timespec(record.timestamp());
        TimeRange[] ranges = { new TimeRange(ts, ts.plusNanos(1)) };
        Reader reader = Table.reader(TestUtils.createSession(), tableFromColumnName, ranges);
        assertEquals(true, reader.hasNext());




        this.task.stop();
    }


    /**
     * Tests that a table can be derived from a composite of multiple columns
     */
    @ParameterizedTest
    @MethodSource("randomRecord")
    public void testResolveTableFromCompositeColumns(Fixture fixture,
                                                     Integer offset,
                                                     Row row,
                                                     SinkRecord record) {
        Map<String, String> props = fixture.props;


        String tableNameDelim   = fixture.tableCompositeColumnDelim[offset];
        String[] tableNameParts = fixture.tableCompositeColumnParts[offset];

        props.put(QdbSinkConfig.TABLE_FROM_COMPOSITE_COLUMNS_CONFIG, String.join(",", Fixture.TABLE_COMPOSITE_COLUMNS_IDS));
        props.put(QdbSinkConfig.TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG, tableNameDelim);
        props.put(QdbSinkConfig.TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG, Fixture.SKELETON_COLUMN_ID);

        this.task.start(props);
        this.task.put(Collections.singletonList(record));
        this.task.flush(new HashMap());

        // Sleep 1 seconds, our flush interval
        try {
            Thread.sleep(1100);
        } catch (Exception e) {
            throw new Error("Unexpected exception", e);
        }

        Timespec ts = new Timespec(record.timestamp());
        TimeRange[] ranges = { new TimeRange(ts, ts.plusNanos(1)) };

        String tableName = String.join(tableNameDelim, tableNameParts);

        Reader reader = Table.reader(TestUtils.createSession(), tableName, ranges);
        assertEquals(true, reader.hasNext());


        this.task.stop();
    }


    /**
     * Tests that a table can insert a single column/value pair
     */
    @ParameterizedTest
    @MethodSource("noSchema")
    public void testInsertSingleColumnValue(Fixture fixture) {
        Map<String, String> props = fixture.props;

        String tableName = TestUtils.createUniqueAlias();
        Column[] columns = new Column[] {
            new Column.Double("bar.foo"),
            new Column.Double("foo.bar"),
            new Column.Double("wom.bat")
        };
        Table t = Table.create(TestUtils.createSession(), tableName, columns);

        props.put(QdbSinkConfig.TABLE_CONFIG, tableName);

        // Hack, but we're abusing our composite table name for a composite column name here. This means we should
        // end up with a single table that has only a single column with a certain value.
        String columnNameDelim   = ".";
        String[] columnNameParts = {"col1", "col2"};

        Map value = new HashMap<String, Object> ();
        value.put("col1", "foo");
        value.put("col2", "bar");
        value.put("value", 12.34);

        Schema schema = null;

        Timespec time = Timespec.now();
        SinkRecord record = new SinkRecord("notopic", 1,
                                           null, null,    // key is unused
                                           schema, value,
                                           -1,            // kafkaOffset
                                           time.toEpochMillis(), TimestampType.CREATE_TIME);

        props.put(QdbSinkConfig.COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG, String.join(",", columnNameParts));
        props.put(QdbSinkConfig.COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG, columnNameDelim);
        props.put(QdbSinkConfig.VALUE_COLUMN_CONFIG, "value");

        this.task.start(props);
        this.task.put(Collections.singletonList(record));
        this.task.flush(new HashMap());

        // Sleep 1 seconds, our flush interval
        try {
            Thread.sleep(1100);
        } catch (Exception e) {
            throw new Error("Unexpected exception", e);
        }

        TimeRange[] ranges = { new TimeRange(time.minusSeconds(15), time.plusSeconds(15)) };
        Reader reader = Table.reader(TestUtils.createSession(), tableName, ranges);
        assertEquals(true, reader.hasNext());


        Row row = reader.next();

        assertEquals(row.getValues().length, 3);
        assertEquals(row.getValues()[1].getDouble(), 12.34);

        this.task.stop();
    }



    /**
     * Parameter provider tests without schema
     */
    static Stream<Arguments> noSchema() throws IOException {
        return Stream.of(Arguments.of(Fixture.of(TestUtils.createSession())));
    }

    /**
     * Parameter provider for random schemas
     */
    static Stream<Arguments> randomSchemaWithValue() throws IOException {
        Fixture fixture       = Fixture.of(TestUtils.createSession()).withRecords(Schema.Type.STRUCT);

        return Stream.of(Arguments.of(fixture, null, null),
                         Arguments.of(fixture, SchemaBuilder.int8(),    (byte)8),
                         Arguments.of(fixture, SchemaBuilder.int16(),   (short)16),
                         Arguments.of(fixture, SchemaBuilder.int32(),   (int)32),
                         Arguments.of(fixture, SchemaBuilder.int64(),   (long)64),
                         Arguments.of(fixture, SchemaBuilder.float32(), (float)32.0),
                         Arguments.of(fixture, SchemaBuilder.float64(), (double)64.0),
                         Arguments.of(fixture, SchemaBuilder.bool(),    true),
                         Arguments.of(fixture, SchemaBuilder.string(), "hi, dave"));
    }

    /**
     * Parameter provider for all rows, grouped per table. Emits exactly
     * NUM_ROWS rows per entry.
     */
    static Stream<Arguments> randomRecords() throws IOException {
        return (IntStream.range(0, SCHEMA_TYPES.length))
            .mapToObj((i) -> {
                    try {
                        return Fixture.of(TestUtils.createSession()).withRecords(SCHEMA_TYPES[i]);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
            .flatMap((fixture) -> {
                    return IntStream.range(0, fixture.records.length)
                        .mapToObj((i) -> {
                                return Arguments.of(fixture, i, Arrays.asList(fixture.records[i]));
                            });
                        });
    }

    /**
     * Parameter provider for single row. Emits a max of 10 rows per table.
     */
    static Stream<Arguments> randomRecord() throws IOException {
        final int MAX_ROWS    = 10;

        // For each struct type, generate a fixture that holds schema/records
        // of that type.
        return (IntStream.range(0, SCHEMA_TYPES.length))
            .mapToObj((i) -> {
                    try {
                        return Fixture.of(TestUtils.createSession()).withRecords(SCHEMA_TYPES[i]);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
            .flatMap((fixture) -> {
                    return IntStream.range(0, Math.min(MAX_ROWS, fixture.records.length))
                        .mapToObj((i) -> {
                                return Arguments.of(i, fixture.rows[i], fixture.records[i]);
                            })
                        .flatMap((args) -> {
                                Integer offset = (Integer)args.get()[0];
                                Row[] rows = (Row[])args.get()[1];
                                SinkRecord[] records = (SinkRecord[])args.get()[2];
                                return IntStream.range(0, Math.min(MAX_ROWS, records.length))
                                    .mapToObj((j) ->
                                              {
                                                  return Arguments.of(fixture,
                                                                      offset,
                                                                      rows[j],
                                                                      records[j]);
                                              });
                            });
                        });
    }
}
