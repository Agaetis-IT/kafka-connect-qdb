package net.quasardb.kafka.common.resolver;

import net.quasardb.kafka.common.config.QdbSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SuffixedResolver extends ColumnResolver<String> {

    private static final Logger log = LoggerFactory.getLogger(SuffixedResolver.class);

    private String suffix;

    public SuffixedResolver(QdbSinkConfig config, String columnName, String suffix) {
        super (config, columnName);
        this.suffix = suffix;
        log.debug("Initializing suffixed column resolver with suffix: {}", suffix);
    }

    @Override
    public String resolve(SinkRecord record) throws DataException {
       String value =  super.resolve(record);

       return this.handleSuffix(value, this.suffix);
    }

    protected String handleSuffix(String result, String suffix){
        return result + suffix;
    }

}
