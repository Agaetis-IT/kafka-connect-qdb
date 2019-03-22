package net.quasardb.kafka.common.writer;

import net.quasardb.kafka.common.RecordConverter;
import net.quasardb.kafka.common.TableInfo;
import net.quasardb.qdb.ts.Timespec;
import net.quasardb.qdb.ts.Value;
import net.quasardb.qdb.ts.Writer;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RowRecordWriter extends RecordWriter {

    private static final Logger log = LoggerFactory.getLogger(RowRecordWriter.class);

    public void write(Writer w, TableInfo t, SinkRecord s) throws RuntimeException {
        Value[] row = RecordConverter.convert (t.getTable().getColumns(), s);

        try {
            Timespec ts = (s.timestamp() == null
                           ? Timespec.now()
                           : new Timespec(s.timestamp()));
            w.append(t.getOffset(), ts, row);
        } catch (Exception e) {
            log.error("Unable to write record: {}", e.getMessage());
            log.error("Record: {}", s);
            throw new RuntimeException(e);
        }
    }

}
