package net.quasardb.kafka.common.resolver;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import net.quasardb.qdb.ts.Timespec;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

public class DefaultTimespecResolver extends Resolver<Timespec> {

    public DefaultTimespecResolver(QdbSinkConfig config) {
        super(config);
    }

    @Override
    public Timespec resolve(SinkRecord record) throws DataException {
        Timespec ts = (record.timestamp() == null
                ? Timespec.now()
                : new Timespec(record.timestamp()));

        return ts;
    }
}
