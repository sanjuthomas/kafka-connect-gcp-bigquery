package kafka.connect.gcp.bigquery.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Sanju Thomas
 *
 */
public class BigQuerySinkConnector extends SinkConnector {

  private static final Logger logger = LoggerFactory.getLogger(SinkConnector.class);
  private Map<String, String> config;

  @Override
  public ConfigDef config() {
    return BigQuerySinkConfig.CONFIG_DEF;
  }

  @Override
  public void start(final Map<String, String> arg0) {
    this.config = arg0;
  }

  @Override
  public void stop() {
    logger.info("stop called");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return BigQuerySinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(final int taskCunt) {
    final List<Map<String, String>> configs = new ArrayList<>(taskCunt);
    for (int i = 0; i < taskCunt; ++i) {
      configs.add(this.config);
    }
    return configs;
  }

  @Override
  public String version() {
    return "1.0";
  }

}