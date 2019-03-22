package net.quasardb.kafka.common.resolver;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicResolver extends Resolver<String> {

    private static final Logger log = LoggerFactory.getLogger(TopicResolver.class);

    public TopicResolver() {
        log.info("Initializing topic table resolver");
    }

    @Override
    public String resolve(SinkRecord record) throws DataException {
        return record.topic();
    }

}
