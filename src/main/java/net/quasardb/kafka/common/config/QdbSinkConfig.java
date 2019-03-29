package net.quasardb.kafka.common.config;

import net.quasardb.kafka.common.resolver.Resolver;
import net.quasardb.kafka.common.writer.RecordWriter;
import net.quasardb.qdb.ts.Table;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Stream.of;
import static net.quasardb.kafka.sink.QdbSinkConnector.DEFAULT_CLUSTER_URI;

public class QdbSinkConfig extends AbstractConfig {

    public static final String CLUSTER_URI_CONFIG = "qdb.cluster";
    public static final String SECURITY_USERNAME_CONFIG = "qdb.security.username";
    public static final String SECURITY_USER_PRIVATE_KEY_CONFIG = "qdb.security.user_private_key";
    public static final String SECURITY_CLUSTER_PUBLIC_KEY_CONFIG = "qdb.security.cluster_public_key";
    public static final String TABLE_CONFIG = "qdb.table";
    public static final String TABLE_FROM_TOPIC_CONFIG = "qdb.table_from_topic";
    public static final String TABLE_FROM_COLUMN_CONFIG = "qdb.table_from_column";
    public static final String TABLE_FROM_CUSTOM_RESOLVER= "qdb.table_from_custom_resolver";
    public static final String TABLE_FROM_COMPOSITE_COLUMNS_CONFIG = "qdb.table_from_columns";
    public static final String TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG = "qdb.table_from_columns_delimiter";
    public static final String TABLE_AUTOCREATE_CONFIG = "qdb.table_autocreate";
    public static final String TABLE_AUTOCREATE_TAGS_CONFIG = "qdb.table_autocreate_tags";
    public static final String TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG = "qdb.table_autocreate_tags_column";
    public static final String TABLE_AUTOCREATE_TAGS_CUSTOM_RESOLVER= "qdb.table_autocreate_tags_custom_resolver";
    public static final String TABLE_AUTOCREATE_SKELETON_CONFIG = "qdb.table_autocreate_skeleton";
    public static final String TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG = "qdb.table_autocreate_skeleton_column";
    public static final String TABLE_AUTOCREATE_SKELETON_CUSTOM_RESOLVER = "qdb.table_autocreate_skeleton_custom_resolver";
    public static final String TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG = "qdb.table_autocreate_skeleton_suffix";
    public static final String TABLE_AUTOCREATE_SHARD_SIZE_CONFIG = "qdb.table_autocreate_shard_size";
    public static final String TABLE_AUTOCREATE_SHARD_SIZE_COLUMN_CONFIG = "qdb.table_autocreate_shard_size_column";
    public static final String TABLE_AUTOCREATE_SHARD_SIZE_CUSTOM_RESOLVER = "qdb.table_autocreate_shard_size_custom_resolver";
    public static final String TIMESTAMP_FROM_COLUMN_CONFIG = "qdb.timestamp_from_column";
    public static final String TIMESTAMP_FROM_COLUMN_UNIT_CONFIG = "qdb.timestamp_from_column_unit";
    public static final String COLUMN_FROM_COLUMN_CONFIG = "qdb.column_from_column";
    public static final String COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG = "qdb.column_from_columns";
    public static final String COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG = "qdb.column_from_columns_delimiter";
    public static final String VALUE_COLUMN_CONFIG = "qdb.value_column";
    public static final String VALUE_FROM_COLUMN_CONFIG = "qdb.value_from_column";
    public static final String CUSTOM_RECORD_WRITER= "qdb.record_writer_custom";

    public static ConfigDef BASE_CONFIG = baseConfigDef();

    public QdbSinkConfig(Map<?, ?> originals, boolean doLog) {
        super(BASE_CONFIG, originals, doLog);
    }

    public QdbSinkConfig(Map<?, ?> originals) {
        super(BASE_CONFIG, originals);
    }

    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(CLUSTER_URI_CONFIG,
                        Type.STRING,
                        DEFAULT_CLUSTER_URI,
                        Importance.HIGH,
                        "The Cluster uri to connect to.")
                .define(SECURITY_USERNAME_CONFIG,
                        Type.STRING,
                        null,
                        Importance.HIGH,
                        "For a secure connection, the username to use.")
                .define(SECURITY_USER_PRIVATE_KEY_CONFIG,
                        Type.STRING,
                        null,
                        Importance.HIGH,
                        "For a secure connection, the user's private key.")
                .define(SECURITY_CLUSTER_PUBLIC_KEY_CONFIG,
                        Type.STRING,
                        null,
                        Importance.HIGH,
                        "For a secure connection, the cluster's public key.")
                .define(TABLE_FROM_TOPIC_CONFIG,
                        Type.BOOLEAN,
                        false,
                        Importance.MEDIUM,
                        "When true, uses Kafka topic id to map directly to QuasarDB table identifier.")
                .define(TABLE_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        "When an identifier is provided, all rows will always be inserted into this table.")
                .define(TABLE_FROM_COLUMN_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        "Identifier of the column to acquire table name from. Cannot be used in combination with qdb.table_from_topic")
                .define(TABLE_FROM_CUSTOM_RESOLVER,
                        Type.CLASS,
                        null,
                        ClassValidator.impl(Resolver.class),
                        Importance.MEDIUM,
                        "Class instance of Resolver used to get table name")
                .define(TABLE_FROM_COMPOSITE_COLUMNS_CONFIG,
                        Type.LIST,
                        null,
                        Importance.MEDIUM,
                        "Identifier of the columns to acquire table name from, joins strings of columns into a table name. Cannot be used in combination with qdb.table_from_topic or qdb.table_from_column.")
                .define(TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG,
                        Type.STRING,
                        "",
                        Importance.MEDIUM,
                        "Optional delimiter to use for joining columns into a single table name.")
                .define(TABLE_AUTOCREATE_CONFIG,
                        Type.BOOLEAN,
                        false,
                        Importance.MEDIUM,
                        "When provided, tries to automatically create tables once they are encountered and not found. Requires either qdb.table_skeleton or qdb.table_skeleton_column to be provided.")
                .define(TABLE_AUTOCREATE_SKELETON_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        "Allows providing of a static 'skeleton' table whose schema will be copied into all autocreated tables.")
                .define(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        "Allows providing of a column which will be used to look up a dynamic 'skeleton' table whose schema will be copied into autocreated tables.")
                .define(TABLE_AUTOCREATE_SKELETON_CUSTOM_RESOLVER,
                        Type.CLASS,
                        null,
                        ClassValidator.impl(Resolver.class),
                        Importance.MEDIUM,
                        "Class instance of Resolver used to get table skeleton name")
                .define(TABLE_AUTOCREATE_SHARD_SIZE_CONFIG,
                        Type.LONG,
                        86400000L, // Default QuasarDB Table shard size.
                        Importance.MEDIUM,
                        "Allows providing of a static 'shardsize' which will be used for all autocreated tables.")
                .define(TABLE_AUTOCREATE_SHARD_SIZE_COLUMN_CONFIG,
                        Type.LONG,
                        null,
                        Importance.MEDIUM,
                        "Allows providing of a column which will be used to look up a dynamic 'shardsize' which will be used for all autocreated tables.")
                .define(TABLE_AUTOCREATE_SHARD_SIZE_CUSTOM_RESOLVER,
                        Type.CLASS,
                        null,
                        ClassValidator.impl(Resolver.class),
                        Importance.MEDIUM,
                        "Class instance of Resolver used to get table shard size")
                .define(TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        "When a skeleton table is derived from a column's value through qdb.table_autocreate_skeleton_column, this allows adding an additional suffix to those.")
                .define(TABLE_AUTOCREATE_TAGS_CONFIG,
                        Type.LIST,
                        null,
                        Importance.MEDIUM,
                        "Allows providing of static additional tags to be assigned to newly created tables.")
                .define(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        "Allows providing of a column which will be used to assign additional tags to newly created tables.")
                .define(TABLE_AUTOCREATE_TAGS_CUSTOM_RESOLVER,
                        Type.CLASS,
                        null,
                        ClassValidator.impl(Resolver.class),
                        Importance.MEDIUM,
                        "Class instance of Resolver used to get table tags")
                .define(TIMESTAMP_FROM_COLUMN_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        "Allows providing of a column which will be used to create row timespec.")
                .define(TIMESTAMP_FROM_COLUMN_UNIT_CONFIG,
                        Type.STRING,
                        TimeUnit.MILLISECONDS.name(),
                        ValidString.in(of(TimeUnit.MILLISECONDS, TimeUnit.MICROSECONDS, TimeUnit.NANOSECONDS).map(TimeUnit::name).toArray(String[]::new)),
                        Importance.MEDIUM,
                        "Allows providing of a TimeUnit precision for timespec column.")
                .define(COLUMN_FROM_COLUMN_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        "For single column/value insertions: identifier of the column to acquire column name from. Cannot be used in combination with qdb.column_from_columns")
                .define(COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG,
                        Type.LIST,
                        null,
                        Importance.MEDIUM,
                        "For single column/value insertions: odentifier of the columns to acquire column name from, joins strings of columns into a column name. Cannot be used in combination with qdb.column_from_column.")
                .define(COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG,
                        Type.STRING,
                        "",
                        Importance.MEDIUM,
                        "Optional delimiter to use for joining columns into a single column name.")
                .define(VALUE_COLUMN_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        "For single column/value insertions: identifier of the column to acquire value from. For example, if set to 'value', the Kafka connector will always look in the column 'value' to find out which value to store.")
                .define(VALUE_FROM_COLUMN_CONFIG,
                        Type.STRING,
                        null,
                        Importance.MEDIUM,
                        "For single column/value insertions: identifier of the column to use as column identifier to acquire value from. For example, if set to 'thecol', the Kafka connector will look up the value of 'thecol', and use that value to retrieve the value, i.e. it allows an additional indirection.")
                .define(CUSTOM_RECORD_WRITER,
                        Type.CLASS,
                        null,
                        ClassValidator.impl(RecordWriter.class),
                        Importance.HIGH.MEDIUM,
                        "")
                ;
    }
}
