package net.quasardb.kafka.common.resolver;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import net.quasardb.qdb.ts.Timespec;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.concurrent.TimeUnit;

public class ColumnTimespecResolver extends Resolver<Timespec> {

    private final TimeUnit unit;

    private final DefaultColumnResolver<Long> resolver;

    public ColumnTimespecResolver(QdbSinkConfig config, String columnName, TimeUnit unit) {
        super(config);
        this.resolver = new DefaultColumnResolver<Long>(config, columnName);
        this.unit = unit;
    }

    @Override
    public Timespec resolve(SinkRecord record) throws DataException {
        final Long value = resolver.resolve(record);

        final long timestamp = unit.toNanos(value);

        long seconds = TimeUnit.NANOSECONDS.toSeconds(timestamp);
        long nanos = timestamp - seconds;

        Timespec ts = new Timespec(seconds, nanos);

        return ts;
    }
}
