package net.quasardb.kafka.common.resolver;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListColumnResolver<T> extends ColumnResolver<List<T> > {

    private static final Logger log = LoggerFactory.getLogger(ListColumnResolver.class);

    public ListColumnResolver(String columnName) {
        super(columnName);
    }

    public ListColumnResolver(String columnName, List<T> suffix) {
        super(columnName, suffix);
    }

    protected List<T> handleSuffix(List<T> result, List<T> suffix) {
        log.warn("Unable to handle suffix for list types");
        return result;
    }
}
