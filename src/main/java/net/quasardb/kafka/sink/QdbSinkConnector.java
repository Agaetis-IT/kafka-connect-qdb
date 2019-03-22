package net.quasardb.kafka.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.quasardb.kafka.common.ConnectorUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QdbSinkConnector extends SinkConnector {

    public static final String DEFAULT_CLUSTER_URI = "qdb://127.0.0.1:2836";

    private static final Logger log = LoggerFactory.getLogger(QdbSinkConnector.class);
    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;

        log.info("Started the QdbSinkConnector.");
    }

    @Override
    public void stop() {
        // TODO Nothing implemented at this point, should probably flush
        log.info("Stopping the QdbSinkConnector.");
    }

    @Override
    public String version() {
        // TODO Currently using Kafka version, in future release use qdb-kafka version
        return AppInfoParser.getVersion();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return QdbSinkTask.class;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define(ConnectorUtils.CLUSTER_URI_CONFIG,
                    Type.STRING,
                    DEFAULT_CLUSTER_URI,
                    Importance.HIGH,
                    "The Cluster uri to connect to.")
            .define(ConnectorUtils.SECURITY_USERNAME_CONFIG,
                    Type.STRING,
                    null,
                    Importance.HIGH,
                    "For a secure connection, the username to use.")
            .define(ConnectorUtils.SECURITY_USER_PRIVATE_KEY_CONFIG,
                    Type.STRING,
                    null,
                    Importance.HIGH,
                    "For a secure connection, the user's private key.")
            .define(ConnectorUtils.SECURITY_CLUSTER_PUBLIC_KEY_CONFIG,
                    Type.STRING,
                    null,
                    Importance.HIGH,
                    "For a secure connection, the cluster's public key.")
            .define(ConnectorUtils.TABLE_FROM_TOPIC_CONFIG,
                    Type.BOOLEAN,
                    false,
                    Importance.MEDIUM,
                    "When true, uses Kafka topic id to map directly to QuasarDB table identifier.")
            .define(ConnectorUtils.TABLE_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "When an identifier is provided, all rows will always be inserted into this table.")
            .define(ConnectorUtils.TABLE_FROM_COLUMN_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "Identifier of the column to acquire table name from. Cannot be used in combination with qdb.table_from_topic")
            .define(ConnectorUtils.TABLE_FROM_COMPOSITE_COLUMNS_CONFIG,
                    Type.LIST,
                    null,
                    Importance.MEDIUM,
                    "Identifier of the columns to acquire table name from, joins strings of columns into a table name. Cannot be used in combination with qdb.table_from_topic or qdb.table_from_column.")
            .define(ConnectorUtils.TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "Optional delimiter to use for joining columns into a single table name.")
            .define(ConnectorUtils.TABLE_AUTOCREATE_CONFIG,
                    Type.BOOLEAN,
                    false,
                    Importance.MEDIUM,
                    "When provided, tries to automatically create tables once they are encountered and not found. Requires either qdb.table_skeleton or qdb.table_skeleton_column to be provided.")
            .define(ConnectorUtils.TABLE_AUTOCREATE_SKELETON_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "Allows providing of a static 'skeleton' table whose schema will be copied into all autocreated tables.")
            .define(ConnectorUtils.TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "Allows providing of a column which will be used to look up a dynamic 'skeleton' table whose schema will be copied into autocreated tables.")
            .define(ConnectorUtils.TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "When a skeleton table is derived from a column's value through qdb.table_autocreate_skeleton_column, this allows adding an additional suffix to those.")
            .define(ConnectorUtils.TABLE_AUTOCREATE_TAGS_CONFIG,
                    Type.LIST,
                    null,
                    Importance.MEDIUM,
                    "Allows providing of static additional tags to be assigned to newly created tables.")
            .define(ConnectorUtils.TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "Allows providing of a column which will be used to assign additional tags to newly created tables.")
            .define(ConnectorUtils.COLUMN_FROM_COLUMN_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "For single column/value insertions: identifier of the column to acquire column name from. Cannot be used in combination with qdb.column_from_columns")
            .define(ConnectorUtils.COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG,
                    Type.LIST,
                    null,
                    Importance.MEDIUM,
                    "For single column/value insertions: odentifier of the columns to acquire column name from, joins strings of columns into a column name. Cannot be used in combination with qdb.column_from_column.")
            .define(ConnectorUtils.COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "Optional delimiter to use for joining columns into a single column name.")
            .define(ConnectorUtils.VALUE_COLUMN_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "For single column/value insertions: identifier of the column to acquire value from. For example, if set to 'value', the Kafka connector will always look in the column 'value' to find out which value to store.")
            .define(ConnectorUtils.VALUE_FROM_COLUMN_CONFIG,
                    Type.STRING,
                    null,
                    Importance.MEDIUM,
                    "For single column/value insertions: identifier of the column to use as column identifier to acquire value from. For example, if set to 'thecol', the Kafka connector will look up the value of 'thecol', and use that value to retrieve the value, i.e. it allows an additional indirection.")
            ;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>(props);
            configs.add(config);
        }
        return configs;
    }

}
