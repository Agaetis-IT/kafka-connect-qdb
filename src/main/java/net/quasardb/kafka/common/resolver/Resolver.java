package net.quasardb.kafka.common.resolver;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Resolver<T> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final QdbSinkConfig config;

    public Resolver(QdbSinkConfig config){
        this.config = config;
    }

    abstract public T resolve(SinkRecord record) throws DataException;

}
