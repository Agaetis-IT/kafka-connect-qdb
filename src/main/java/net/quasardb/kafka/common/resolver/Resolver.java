package net.quasardb.kafka.common.resolver;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public abstract class Resolver<T> {

    abstract public T resolve(SinkRecord record) throws DataException;

}
