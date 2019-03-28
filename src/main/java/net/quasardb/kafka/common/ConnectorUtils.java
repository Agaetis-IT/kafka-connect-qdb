package net.quasardb.kafka.common;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import net.quasardb.kafka.common.resolver.ColumnsResolver;
import net.quasardb.kafka.common.resolver.ListColumnResolver;
import net.quasardb.kafka.common.resolver.LongColumnResolver;
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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.quasardb.kafka.common.config.QdbSinkConfig.*;
import static net.quasardb.kafka.common.config.QdbSinkConfig.CLUSTER_URI_CONFIG;
import static net.quasardb.kafka.common.config.QdbSinkConfig.SECURITY_USERNAME_CONFIG;

public class ConnectorUtils {

    private static final Logger log = LoggerFactory.getLogger(ConnectorUtils.class);

    /**
     * Parses table input config and emits a mapping of Kafka topics to
     * QuasarDB tables.
     */
    public static Map<String, String> parseTableFromTopic(Collection<String> xs) {
        Map<String, String> out = new HashMap(xs.size());

        for (String x : xs) {
            String[] tokens = x.split("=");

            if (tokens.length != 2) {
                throw new DataException("Incorrectly formatted table config: expected 'topic=table', got: " + x);
            }

            out.put(tokens[0], tokens[1]);
        }

        return out;
    }

    public static Session connect(QdbSinkConfig config) {
        String uri = config.getString(CLUSTER_URI_CONFIG);

        if (config.getString(SECURITY_USERNAME_CONFIG) != null &&
                config.getString(SECURITY_USER_PRIVATE_KEY_CONFIG) != null &&
                config.getString(SECURITY_CLUSTER_PUBLIC_KEY_CONFIG) != null) {

            String userName = config.getString(SECURITY_USERNAME_CONFIG);
            String userPrivateKey = config.getString(SECURITY_USER_PRIVATE_KEY_CONFIG);
            String clusterPublicKey = config.getString(SECURITY_CLUSTER_PUBLIC_KEY_CONFIG);
            log.info("Establishing secure connection to {}", uri);

            final Session.SecurityOptions options = new Session.SecurityOptions(userName, userPrivateKey, clusterPublicKey);
            return Session.connect(options, uri);
        } else {
            log.warn("Establishing insecure connection to {}", uri);
            return Session.connect(uri);
        }
    }

    public static Resolver<String> createTableResolver(QdbSinkConfig config) {
        if (config.getString(TABLE_FROM_COLUMN_CONFIG) != null) {
            log.debug("{} set, using ColumnResolver", TABLE_FROM_COLUMN_CONFIG);
            return new StringColumnResolver(config.getString(TABLE_FROM_COLUMN_CONFIG));
        } else if (config.getList(TABLE_FROM_COMPOSITE_COLUMNS_CONFIG) != null) {
            log.debug("{} set, using ColumnsResolver", TABLE_FROM_COMPOSITE_COLUMNS_CONFIG);
            List<String> columns = config.getList(TABLE_FROM_COMPOSITE_COLUMNS_CONFIG);
            if (config.getString(TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG) != null) {
                log.debug("{} set, using a delimiter", TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG);
                return new ColumnsResolver(columns, config.getString(TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG));
            } else {
                log.debug("{} not set, not using a delimiter", TABLE_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG);
                return new ColumnsResolver(columns);
            }
        } else if (config.getBoolean(TABLE_FROM_TOPIC_CONFIG)) {
            log.debug("{} set to true, using TopicResolver", TABLE_FROM_TOPIC_CONFIG);
            return new TopicResolver();
        } else if (config.getString(TABLE_CONFIG) != null) {
            log.debug("{} provided, using StaticTableResolver", TABLE_CONFIG);
            return new StaticResolver<>(config.getString(TABLE_CONFIG));
        } else {
            log.debug("validatedProps: {}", config);
            throw new DataException("No valid TableResolving strategy could be determined, please correct your configuration");
        }
    }

    public static Resolver<String> createSkeletonTableResolver(QdbSinkConfig config) {
        if (config.getString(TABLE_AUTOCREATE_SKELETON_CONFIG) != null) {
            log.debug("{} provided", TABLE_AUTOCREATE_SKELETON_CONFIG);
            return new StaticResolver<String>(config.getString(TABLE_AUTOCREATE_SKELETON_CONFIG));
        } else if (config.getString(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG) != null) {
            log.debug("{} provided", TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG);
            if (config.getString(TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG) != null) {
                log.debug("{} provided", TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG);
                return new StringColumnResolver(config.getString(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG),
                        config.getString(TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG));
            } else {
                log.debug("{} not provided", TABLE_AUTOCREATE_SKELETON_SUFFIX_CONFIG);
                return new StringColumnResolver(config.getString(TABLE_AUTOCREATE_SKELETON_COLUMN_CONFIG));
            }

        } else {
            log.debug("No skeleton configuration");
            return null;
        }
    }

    public static Resolver<List<String>> createTableTagsResolver(QdbSinkConfig config) {
        if (config.getList(TABLE_AUTOCREATE_TAGS_CONFIG) != null) {
            log.debug("{} provided, using StaticResolver", TABLE_AUTOCREATE_TAGS_CONFIG);
            return new StaticResolver<>(config.getList(TABLE_AUTOCREATE_TAGS_CONFIG));
        } else if (config.getString(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG) != null) {
            log.debug(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG + " provided, using ColumnResolver");
            return new ListColumnResolver<>(config.getString(TABLE_AUTOCREATE_TAGS_COLUMN_CONFIG));
        } else {
            log.debug("No table tags configuration");
            return null;
        }
    }

    public static Resolver<Long> createShardSizeResolver(QdbSinkConfig config) {
        if (config.getLong(TABLE_AUTOCREATE_SHARD_SIZE_CONFIG) != null) {
            log.debug("{} provided, using StaticResolver", TABLE_AUTOCREATE_SHARD_SIZE_CONFIG);
            return new StaticResolver<>(config.getLong(TABLE_AUTOCREATE_SHARD_SIZE_CONFIG));
        } else if (config.getString(TABLE_AUTOCREATE_SHARD_SIZE_COLUMN_CONFIG) != null) {
            log.debug("{} provided, using ColumnResolver", TABLE_AUTOCREATE_SHARD_SIZE_COLUMN_CONFIG);
            return new LongColumnResolver(config.getString(TABLE_AUTOCREATE_SHARD_SIZE_COLUMN_CONFIG));
        } else {
            log.debug("No table shard size configuration");
            return null;
        }
    }

    public static RecordWriter createRecordWriter(QdbSinkConfig config) {
        if ((config.getString(COLUMN_FROM_COLUMN_CONFIG) != null)
                || (config.getList(COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG) != null)
                || (config.getString(VALUE_COLUMN_CONFIG) != null)
                || (config.getString(VALUE_FROM_COLUMN_CONFIG) != null)) {
            log.debug("enabling column value resolver");

            Resolver<String> columnResolver;

            if (config.getString(COLUMN_FROM_COLUMN_CONFIG) != null) {
                columnResolver = new StringColumnResolver(config.getString(COLUMN_FROM_COLUMN_CONFIG));
            } else if (config.getList(COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG) != null) {
                List<String> columns = config.getList(COLUMN_FROM_COMPOSITE_COLUMNS_CONFIG);
                if (config.getString(COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG) != null) {
                    log.debug("{} set, using a delimiter", COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG);
                    columnResolver = new ColumnsResolver(columns, (COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG));
                } else {
                    log.debug("{} not set, not using a delimiter", COLUMN_FROM_COMPOSITE_COLUMNS_DELIM_CONFIG);
                    columnResolver = new ColumnsResolver(columns);
                }
            } else {
                log.error("Unable to determine a column resolver for column value resolver");
                return null;
            }

            Resolver<String> valueResolver;
            if (config.getString(VALUE_COLUMN_CONFIG) != null) {
                valueResolver = new StaticResolver<>(config.getString(VALUE_COLUMN_CONFIG));
            } else if (config.getString(VALUE_FROM_COLUMN_CONFIG) != null) {
                valueResolver = new StringColumnResolver(config.getString(VALUE_FROM_COLUMN_CONFIG));
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
