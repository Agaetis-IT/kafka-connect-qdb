package net.quasardb.kafka.common;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.quasardb.kafka.common.resolver.ColumnsResolver;
import net.quasardb.kafka.common.resolver.ListColumnResolver;
import net.quasardb.kafka.common.resolver.Resolver;
import net.quasardb.kafka.common.resolver.StaticResolver;
import net.quasardb.kafka.common.resolver.StringColumnResolver;
import net.quasardb.kafka.common.resolver.TopicResolver;
import net.quasardb.kafka.common.writer.ColumnRecordWriter;
import net.quasardb.kafka.common.writer.RecordWriter;
import net.quasardb.kafka.common.writer.RowRecordWriter;
import net.quasardb.qdb.Session;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectorUtils {

    private static final Logger log = LoggerFactory.getLogger(ConnectorUtils.class);

    public static final String CLUSTER_URI_CONFIG = "qdb.cluster";
    public static final String SECURITY_USERNAME_CONFIG = "qdb.security.username";
    public static final String SECURITY_USER_PRIVATE_KEY_CONFIG = "qdb.security.user_private_key";
    public static final String SECURITY_CLUSTER_PUBLIC_KEY_CONFIG = "qdb.security.cluster_public_key";

    public static final String TABLE_CONFIG = "qdb.table";
    public static final String TABLE_FROM_TOPIC_CONFIG = "qdb.table_from_topic";
    public static final String TABLE_FROM_COLUMN_CONFIG = "qdb.table_from_column";
    public static final String TABLE_FROM_COMPOSITE_COLUMNS_CONFIG = "qdb.table_from_columns";
    public static final String TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG = "qdb.table_from_columns_delimiter";

    public static final String TABLE_AUTOCREATE_CONFIG = "qdb.table_autocreate";
    public static final String TABLE_AUTOCREATE_TAGS_CONFIG = "qdb.table_autocreate_tags";
    public static final String TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG = "qdb.table_autocreate_tags_column";
    public static final String TABLE_AUTOCREATE_SKELETON_CONFIG = "qdb.table_autocreate_skeleton";
    public static final String TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG = "qdb.table_autocreate_skeleton_column";
    public static final String TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG = "qdb.table_autocreate_skeleton_suffix";

    public static final String TIMESTAMP_FROM_COLUMN_CONFIG = "qdb.timestamp_from_column";
    public static final String TIMESTAMP_FROM_COLUMN_FORMAT_CONFIG = "qdb.timestamp_from_column_format";

    public static final String COLUMN_FROM_COLUMN_CONFIG = "qdb.column_from_column";
    public static final String COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG = "qdb.column_from_columns";
    public static final String COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG = "qdb.column_from_columns_delimiter";
    public static final String VALUE_COLUMN_CONFIG = "qdb.value_column";
    public static final String VALUE_FROM_COLUMN_CONFIG = "qdb.value_from_column";


    /**
     * Parses table input config and emits a mapping of Kafka topics to
     * QuasarDB tables.
     */
    public static Map<String, String> parseTableFromTopic(Collection<String> xs) {
        Map<String, String> out = new HashMap();

        for (String x : xs) {
            String[] tokens = x.split("=");

            if (tokens.length != 2) {
                throw new DataException("Incorrectly formatted table config: expected 'topic=table', got: " + x);
            }

            out.put(tokens[0], tokens[1]);
        }

        return out;
    }

    public static Session connect(Map <String, Object> validatedProps) {
        String uri = (String)validatedProps.get(ConnectorUtils.CLUSTER_URI_CONFIG);

        if (validatedProps.containsKey(SECURITY_USERNAME_CONFIG) &&
            validatedProps.containsKey(SECURITY_USER_PRIVATE_KEY_CONFIG) &&
            validatedProps.get(SECURITY_USER_PRIVATE_KEY_CONFIG) != null &&
            validatedProps.containsKey(SECURITY_CLUSTER_PUBLIC_KEY_CONFIG) &&
            validatedProps.get(SECURITY_CLUSTER_PUBLIC_KEY_CONFIG) != null) {
            String userName = (String)validatedProps.get(SECURITY_USERNAME_CONFIG);
            String userPrivateKey = (String)validatedProps.get(SECURITY_USER_PRIVATE_KEY_CONFIG);
            String clusterPublicKey = (String)validatedProps.get(SECURITY_CLUSTER_PUBLIC_KEY_CONFIG);
            log.info("Establishing secure connection");
            return Session.connect(new Session.SecurityOptions(userName,
                                                               userPrivateKey,
                                                               clusterPublicKey),
                                   uri);
        } else {
            log.warn("Establishing insecure connection");
            return Session.connect(uri);
        }
    }
    public static Resolver<String> createTableResolver(Map <String, Object> validatedProps) {

        System.err.println("validatedProps = " + validatedProps.toString());

        if (validatedProps.containsKey(TABLE_FROM_COLUMN_CONFIG) &&
            validatedProps.get(TABLE_FROM_COLUMN_CONFIG) != null) {
            log.debug("{} set, using ColumnResolver",TABLE_FROM_COLUMN_CONFIG);
            return new StringColumnResolver((String)validatedProps.get(TABLE_FROM_COLUMN_CONFIG));
        } else if (validatedProps.containsKey(TABLE_FROM_COMPOSITE_COLUMNS_CONFIG) &&
                   validatedProps.get(TABLE_FROM_COMPOSITE_COLUMNS_CONFIG) != null) {
            log.debug("{} set, using ColumnsResolver",TABLE_FROM_COMPOSITE_COLUMNS_CONFIG);
            List<String> columns = (List<String>)validatedProps.get(TABLE_FROM_COMPOSITE_COLUMNS_CONFIG);
            if (validatedProps.containsKey(TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG) &&
                validatedProps.get(TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG) != null) {
                log.debug("{} set, using a delimiter",TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG);
                return new ColumnsResolver(columns,
                                           (String)validatedProps.get(TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG));
            } else {
                log.debug("{} not set, not using a delimiter", TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG);
                return new ColumnsResolver(columns);
            }
        } else if (validatedProps.containsKey(TABLE_FROM_TOPIC_CONFIG) &&
                   (Boolean)validatedProps.get(TABLE_FROM_TOPIC_CONFIG) == Boolean.TRUE) {
            log.debug("{} set to true, using TopicResolver",TABLE_FROM_TOPIC_CONFIG);
            return new TopicResolver();
        } else if (validatedProps.containsKey(TABLE_CONFIG) &&
                   validatedProps.get(TABLE_CONFIG) != null) {
            log.debug("{} provided, using StaticTableResolver",TABLE_CONFIG);
            return new StaticResolver<String>((String)validatedProps.get(TABLE_CONFIG));
        } else  {
            log.debug("validatedProps: {}", validatedProps);
            throw new DataException("No valid TableResolving strategy could be determined, please correct your configuration");
        }
    }

    public static Resolver<String> createSkeletonTableResolver(Map <String, Object> validatedProps) {
        if (validatedProps.containsKey(TABLE_AUTOCREATE_SKELETON_CONFIG) &&
            validatedProps.get(TABLE_AUTOCREATE_SKELETON_CONFIG) != null) {
            log.debug("{} provided",TABLE_AUTOCREATE_SKELETON_CONFIG);
            return new StaticResolver<String>((String)validatedProps.get(TABLE_AUTOCREATE_SKELETON_CONFIG));
        } else if (validatedProps.containsKey(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG) &&
                   validatedProps.get(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG) != null) {
            log.debug("{} provided",TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG);

            if (validatedProps.containsKey(TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG) &&
                validatedProps.get(TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG) != null) {
                log.debug( "{} provided",TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG);
                return new StringColumnResolver((String)validatedProps.get(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG),
                                                (String)validatedProps.get(TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG));
            } else {
                log.debug("{} not provided",TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG);
                return new StringColumnResolver((String)validatedProps.get(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG));
            }

        } else {
            log.debug("No skeleton configuration");
            return null;
        }
    }

    public static Resolver<List<String>> createTableTagsResolver(Map <String, Object> validatedProps) {
        if (validatedProps.containsKey(TABLE_AUTOCREATE_TAGS_CONFIG) &&
            validatedProps.get(TABLE_AUTOCREATE_TAGS_CONFIG) != null) {
            log.debug("{} provided, using StaticResolver",TABLE_AUTOCREATE_TAGS_CONFIG);
            return new StaticResolver<>((List<String>)validatedProps.get(TABLE_AUTOCREATE_TAGS_CONFIG));
        } else if (validatedProps.containsKey(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG) &&
                   validatedProps.get(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG) != null) {
            log.debug(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG + " provided, using ColumnResolver");
            return new ListColumnResolver<String>((String)validatedProps.get(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG));
        } else {
            log.debug("No table tags configuration");
            return null;
        }
    }

    public static RecordWriter createRecordWriter(Map <String, Object> validatedProps) {
        if ((validatedProps.containsKey(COLUMN_FROM_COLUMN_CONFIG) && validatedProps.get(COLUMN_FROM_COLUMN_CONFIG) != null)
            || (validatedProps.containsKey(COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG) && validatedProps.get(COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG) != null)
            || (validatedProps.containsKey(VALUE_COLUMN_CONFIG) && validatedProps.get(VALUE_FROM_COLUMN_CONFIG) != null)
            || (validatedProps.containsKey(VALUE_FROM_COLUMN_CONFIG) && validatedProps.get(VALUE_FROM_COLUMN_CONFIG) != null)) {
            log.debug("enabling column value resolver");

            Resolver<String> columnResolver;

            if (validatedProps.containsKey(COLUMN_FROM_COLUMN_CONFIG) && validatedProps.get(COLUMN_FROM_COLUMN_CONFIG) != null) {
                columnResolver = new StringColumnResolver((String)validatedProps.get(COLUMN_FROM_COLUMN_CONFIG));
            } else if (validatedProps.containsKey(COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG) && validatedProps.get(COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG) != null) {
                List<String> columns = (List<String>)validatedProps.get(COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG);
                if (validatedProps.containsKey(COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG) &&
                    validatedProps.get(COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG) != null) {
                    log.debug("{} set, using a delimiter",COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG);
                    columnResolver = new ColumnsResolver(columns,
                                                         (String)validatedProps.get(COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG));
                } else {
                    log.debug("{} not set, not using a delimiter",COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG);
                    columnResolver = new ColumnsResolver(columns);
                }
            } else {
                log.error("Unable to determine a column resolver for column value resolver");
                return null;
            }

            Resolver<String> valueResolver;
            if (validatedProps.containsKey(VALUE_COLUMN_CONFIG) && validatedProps.get(VALUE_COLUMN_CONFIG) != null) {
                valueResolver = new StaticResolver<String>((String)validatedProps.get(VALUE_COLUMN_CONFIG));
            } else if (validatedProps.containsKey(VALUE_FROM_COLUMN_CONFIG) && validatedProps.get(VALUE_FROM_COLUMN_CONFIG) != null) {
                valueResolver = new StringColumnResolver((String)validatedProps.get(VALUE_FROM_COLUMN_CONFIG));
            } else {
                log.error("Unable to determine a value resolver for column value resolver");
                return null;
            }

            return new ColumnRecordWriter(columnResolver, valueResolver);
        } else {
            return new RowRecordWriter();
        }
    }
}
