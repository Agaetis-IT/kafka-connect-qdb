package net.quasardb.kafka.common.writer;

import net.quasardb.kafka.common.TableInfo;
import net.quasardb.qdb.ts.Writer;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

abstract public class RecordWriter {

    abstract public void write(Writer w, TableInfo t, SinkRecord s) throws DataException, RuntimeException;

}
