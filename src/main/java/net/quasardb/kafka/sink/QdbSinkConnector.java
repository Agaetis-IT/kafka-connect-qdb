package net.quasardb.kafka.sink;

import net.quasardb.kafka.common.ConnectorUtils;
import net.quasardb.kafka.common.config.QdbSinkConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
        return QdbSinkConfig.BASE_CONFIG;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>(props);
            configs.add(config);
        }
        return configs;
    }

}
