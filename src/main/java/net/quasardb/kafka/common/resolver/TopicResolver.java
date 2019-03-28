package net.quasardb.kafka.common.resolver;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class TopicResolver extends Resolver<String> {

    public TopicResolver(QdbSinkConfig config) {
        super(config);
        log.debug("Initializing topic table resolver");
    }

    @Override
    public String resolve(SinkRecord record) throws DataException {
        return record.topic();
    }
}
