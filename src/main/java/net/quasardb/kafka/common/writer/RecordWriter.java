package net.quasardb.kafka.common.writer;

import net.quasardb.kafka.common.TableInfo;
import net.quasardb.kafka.common.resolver.Resolver;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Writer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class RecordWriter {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected final Resolver<Timespec> timespecResolver;

    public RecordWriter(Resolver<Timespec> timespecResolver) {
        this.timespecResolver = timespecResolver;
    }

    abstract public void write(Writer w, TableInfo t, SinkRecord s) throws RuntimeException;

}
