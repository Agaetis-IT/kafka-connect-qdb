package net.quasardb.kafka.common.resolver;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class StaticResolver<T> extends Resolver<T> {

    private T value;

    public StaticResolver(QdbSinkConfig config, T value) {
        super(config);
        this.value = value;
        log.debug("Initializing static resolver with value '{}'", value);
    }


    @Override
    public T resolve(SinkRecord record) throws DataException {
        return this.value;
    }
}
