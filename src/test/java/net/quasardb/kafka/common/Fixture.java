package net.quasardb.kafka.common;

import java.io.IOException;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.Column;
import net.quasardb.qdb.ts.Row;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.Value;


public class Fixture implements Cloneable {
    public static final String   SKELETON_COLUMN_ID          = "skeleton_table";
    public static final String   TABLE_COLUMN_ID             = "table_id";
    public static final String[] TABLE_COMPOSITE_COLUMNS_IDS = {"table_id1", "table_id2"};
    public static final String   TAGS_COLUMN_ID              = "table_tags";

    private static final int     NUM_TABLES  = 1;
    private static final int     NUM_ROWS    = 100;
    private static Value.Type[]  VALUE_TYPES = { Value.Type.INT64,
                                                 Value.Type.DOUBLE,
                                                 Value.Type.BLOB };

    public Column[][]          columns;
    public Row[][]             rows;
    public Table[]             tables;
    public String[]            tableFromColumnName;
    public String[][]          tableCompositeColumnParts;
    public String[]            tableCompositeColumnDelim;
    public String[][]          tags;
    public Schema[]            schemas;
    public SinkRecord[][]      records;
    public Map<String, String> props;

    public Fixture() {
        this.columns                    = new Column[NUM_TABLES][];
        this.rows                       = new Row[NUM_TABLES][];
        this.tables                     = new Table[NUM_TABLES];
        this.tableFromColumnName        = new String[NUM_TABLES];
        this.tableCompositeColumnParts  = new String[NUM_TABLES][];
        this.tableCompositeColumnDelim  = new String[NUM_TABLES];
        this.tags                       = new String[NUM_TABLES][];
        this.schemas                    = new Schema[NUM_TABLES];
        this.records                    = new SinkRecord[NUM_TABLES][];
        this.props                      = new HashMap<String, String>();
    }

    /**
     * Copy constructor
     */
    public Fixture(Fixture in) {
        this.columns                   = in.columns;
        this.rows                      = in.rows;
        this.tables                    = in.tables;
        this.tableFromColumnName       = in.tableFromColumnName;
        this.tableCompositeColumnParts = in.tableCompositeColumnParts;
        this.tableCompositeColumnDelim = in.tableCompositeColumnDelim;
        this.tags                      = in.tags;
        this.schemas                   = in.schemas;
        this.records                   = in.records;
        this.props                     = in.props;
    }

    public static Fixture of(Session session) throws IOException {
        Fixture out = new Fixture();

        for (int i = 0; i < NUM_TABLES; ++i) {

            // Generate a column of each value type
            out.columns[i] = Arrays.stream(VALUE_TYPES)
                .map((type) -> {
                        return TestUtils.generateTableColumn(type);
                    })
                .toArray(Column[]::new);
            out.rows[i] = TestUtils.generateTableRows(out.columns[i], NUM_ROWS);
            out.tables[i] = TestUtils.createTable(session, out.columns[i]);
            out.tableFromColumnName[i] = TestUtils.createUniqueAlias(); // Generate unique, alternative table name from each table
            out.tableCompositeColumnParts[i] = new String[] { TestUtils.createUniqueAlias(),
                                                              TestUtils.createUniqueAlias() };
            out.tableCompositeColumnDelim[i] = ".";
            out.tags[i] = new String[] { TestUtils.createUniqueAlias(),
                                         TestUtils.createUniqueAlias() };
        }

        String topicMap = Arrays.stream(out.tables)
            .map((table) -> {
                    // Here we assume kafka topic id == qdb table id
                    return table.getName() + "=" + table.getName();
                })
            .collect(Collectors.joining(","));

        out.props.put(QdbSinkConfig.CLUSTER_URI_CONFIG, TestUtils.CLUSTER_URI);
        out.props.put(QdbSinkConfig.SECURITY_USERNAME_CONFIG, TestUtils.SECURITY_USERNAME);
        out.props.put(QdbSinkConfig.SECURITY_USER_PRIVATE_KEY_CONFIG, TestUtils.SECURITY_USER_PRIVATE_KEY);
        out.props.put(QdbSinkConfig.SECURITY_CLUSTER_PUBLIC_KEY_CONFIG, TestUtils.SECURITY_CLUSTER_PUBLIC_KEY);

        return out;
    }

    public Fixture withRecords(Schema.Type schemaType) {
        Fixture out = new Fixture(this);

        for (int i = 0; i < NUM_TABLES; ++i) {
            // Only using 'schemaless' values for now
            out.schemas[i]         = TestUtils.columnsToSchema(schemaType,
                                                               SKELETON_COLUMN_ID,
                                                               TABLE_COLUMN_ID,
                                                               TABLE_COMPOSITE_COLUMNS_IDS,
                                                               TAGS_COLUMN_ID,
                                                               out.columns[i]);

            final String[] tags           = out.tags[i];
            final Table table             = out.tables[i];
            final String tableName        = out.tableFromColumnName[i];
            final String[] tableNameParts = out.tableCompositeColumnParts[i];
            final String tablePartsDelim  = out.tableCompositeColumnDelim[i];
            final Schema schema           = out.schemas[i];
            final String topic            = out.tables[i].getName();
            final Column[] columns        = out.columns[i];

            out.records[i] = Arrays.stream(out.rows[i])
                .map((row) -> {
                        try {
                            return TestUtils.rowToRecord(topic,
                                                         0,
                                                         table,
                                                         SKELETON_COLUMN_ID,
                                                         tableName,
                                                         TABLE_COLUMN_ID,
                                                         tableNameParts,
                                                         TABLE_COMPOSITE_COLUMNS_IDS,
                                                         schema,
                                                         Arrays.asList(tags),
                                                         TAGS_COLUMN_ID,
                                                         columns,
                                                         row);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                .toArray(SinkRecord[]::new);
        }

        return out;
    }
}
