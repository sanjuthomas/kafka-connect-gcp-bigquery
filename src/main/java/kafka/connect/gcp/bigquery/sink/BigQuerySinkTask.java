package kafka.connect.gcp.bigquery.sink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Sanju Thomas
 *
 */
public class BigQuerySinkTask extends SinkTask {

  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkTask.class);

  @Override
  public String version() {
    return "0.1";
  }

  @Override
  public void put(final Collection<SinkRecord> sinkRecords) {
    logger.info("Data arrived in the Bigtable Sink Task, the count is {}", sinkRecords.size());
  }

  @Override
  public void start(final Map<String, String> config) {}

  @Override
  public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {}

  @Override
  public void stop() {
    logger.info("{} stopped", this);
  }

}