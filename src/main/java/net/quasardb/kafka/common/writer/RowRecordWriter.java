package net.quasardb.kafka.common.writer;

import net.quasardb.kafka.common.RecordConverter;
import net.quasardb.kafka.common.TableInfo;
import net.quasardb.kafka.common.resolver.Resolver;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Writer;
import org.apache.kafka.connect.sink.SinkRecord;

public class RowRecordWriter extends RecordWriter {

    public RowRecordWriter(Resolver<Timespec> timespecResolver) {
        super(timespecResolver);
    }

    public void write(Writer w, TableInfo t, SinkRecord s) throws RuntimeException {
        Value[] row = RecordConverter.convert(t.getTable().getColumns(), s);

        try {
            Timespec ts = timespecResolver.resolve(s);

            w.append(t.getOffset(), ts, row);
        } catch (Exception e) {
            log.error("Unable to write record: {}", e.getMessage());
            log.error("Record: {}", s);
            throw new RuntimeException(e);
        }
    }

}
