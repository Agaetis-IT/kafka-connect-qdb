package net.quasardb.kafka.sink;

import net.quasardb.kafka.common.ConnectorUtils;
import net.quasardb.kafka.common.TableInfo;
import net.quasardb.kafka.common.TableRegistry;
import net.quasardb.kafka.common.config.QdbSinkConfig;
import net.quasardb.kafka.common.resolver.Resolver;
import net.quasardb.kafka.common.writer.RecordWriter;
import net.quasardb.qdb.Session;
import net.quasardb.qdb.ts.Table;
import net.quasardb.qdb.ts.Writer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class QdbSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(QdbSinkTask.class);

    private Session session;
    private Writer writer;

    private TableRegistry tableRegistry;
    private RecordWriter recordWriter;
    private Resolver<String> tableResolver;
    private Resolver<String> skeletonTableResolver;
    private Resolver<List<String>> tableTagsResolver;
    private Resolver<Long> tableShardSizeResolver;

    private QdbSinkConfig config;

    /**
     * Always use no-arg constructor, #start will initialize the task.
     */
    public QdbSinkTask() {
    }

    @Override
    public String version() {
        return new QdbSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        if (this.writer != null) {
            throw new RuntimeException("can only start a task once");
        }

        this.tableRegistry = new TableRegistry();

        config = new QdbSinkConfig(props, log.isDebugEnabled());

        this.session = ConnectorUtils.connect(config);
        this.tableResolver = ConnectorUtils.createTableResolver(config);
        this.skeletonTableResolver = ConnectorUtils.createSkeletonTableResolver(config);
        this.tableTagsResolver = ConnectorUtils.createTableTagsResolver(config);
        this.tableShardSizeResolver = ConnectorUtils.createShardSizeResolver(config);
        this.recordWriter = ConnectorUtils.createRecordWriter(config);

        log.info("Started QdbSinkTask");
    }

    @Override
    public void stop() {
        log.info("Stopping QdbSinkTask");

        try {
            if (this.writer != null) {
                this.writer.close();
                this.writer = null;
            }

            if (this.session != null) {
                this.session.close();
                this.session = null;
            }

            this.tableRegistry = null;
            this.tableResolver = null;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Table createTable(String tableName, SinkRecord record) throws DataException {
        // Table not found
        if (this.skeletonTableResolver == null) {
            throw new DataException("Table '" + tableName + "' not found, and no skeleton table configuration for creation, aborting");
        }

        Table skeleton = new Table(this.session, this.skeletonTableResolver.resolve(record));
        log.info("creating copy of skeleton table '{}' into target table '{}'", skeleton.getName(), tableName);

        Long shardsize = this.tableShardSizeResolver.resolve(record);
        log.debug("using shard size {} for table {}", shardsize, tableName);

        Table table = Table.create(this.session, tableName, skeleton, shardsize);

        if (this.tableTagsResolver != null) {
            List<String> tags = this.tableTagsResolver.resolve(record);
            log.debug("attaching tags {} to table {}", tags, tableName);
            Table.attachTags(this.session, tableName, tags);
        }

        return table;
    }

    private TableInfo addTableToRegistry(String tableName, SinkRecord record) throws DataException {
        if (tableName == null) {
            throw new DataException("Invalid table name provided: " + tableName);
        }

        log.info("Adding table to registry: {}", tableName);

        TableInfo t = this.tableRegistry.put(this.session, tableName);
        if (t == null) {
            t = this.tableRegistry.put(this.createTable(tableName, record));
        }

        if (this.writer == null) {
            boolean auto = config.getBoolean(QdbSinkConfig.WRITER_MODE_AUTOFLUSH);
            boolean async = config.getBoolean(QdbSinkConfig.WRITER_MODE_ASYNC);

            log.debug("Initializing Writer (Async:{} & AutoFlush:{})", async, auto);
            if (auto) {
                if (async)
                    this.writer = Table.asyncAutoFlushWriter(this.session, t.getTable());
                else
                    this.writer = Table.autoFlushWriter(this.session, t.getTable());

            } else if (async)
                this.writer = Table.asyncWriter(this.session, t.getTable());
            else
                this.writer = Table.writer(this.session, t.getTable());
        } else {
            log.debug("Writer already initialized, adding extra table");
            this.writer.extraTables(t.getTable());
        }

        return t;
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord s : sinkRecords) {
            String tableName = this.tableResolver.resolve(s);
            TableInfo t = this.tableRegistry.get(tableName);

            if (t == null) {
                t = addTableToRegistry(tableName, s);
            }

            if (t.hasOffset() == false) {
                t.setOffset(this.writer.tableIndexByName(t.getTable().getName()));
            }

            this.recordWriter.write(this.writer, t, s);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> partitionOffsets) {
        try {
            if (this.writer != null) {
                log.info("Flush request received, flushing writer.");
                this.writer.flush();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
