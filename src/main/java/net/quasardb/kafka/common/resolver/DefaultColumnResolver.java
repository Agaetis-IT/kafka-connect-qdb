package net.quasardb.kafka.common.resolver;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultColumnResolver<T> extends ColumnResolver<T> {

    public DefaultColumnResolver(QdbSinkConfig config, String columnName) {
        super(config, columnName);
        log.debug("Initializing default column resolver");
    }
}
