package net.quasardb.kafka.common.resolver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongColumnResolver extends ColumnResolver<Long> {

  private static final Logger log = LoggerFactory.getLogger(LongColumnResolver.class);

  public LongColumnResolver(String columnName) {
    super(columnName);
  }

  @Override
  protected Long handleSuffix(Long result, Long suffix) {
    log.warn("Unable to handle suffix for long type");
    return result;
  }
}
