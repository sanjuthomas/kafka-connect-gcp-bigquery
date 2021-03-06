package kafka.connect.gcp.bigquery.writer;

import com.google.cloud.bigquery.Field;
import java.util.LinkedHashMap;
import java.util.Map;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.TableField;
import com.google.cloud.bigquery.BigQuery.TableOption;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

import kafka.connect.gcp.bigquery.config.TableConfig;

/**
 * @author Sanju Thomas
 */
public class SchemaReader {

    private final BigQuery bigQuery;

    public SchemaReader(final BigQuery bigQuery) {
        this.bigQuery = bigQuery;
    }

    public Map<String, Field> tableSchemaFor(final TableConfig tableConfig) {
        final Table tableMetadata = bigQuery
            .getTable(TableId.of(tableConfig.dataset(), tableConfig.name()),
                TableOption.fields(TableField.SCHEMA));
        final Map<String, Field> filedMap = new LinkedHashMap<>();
        if (null != tableMetadata && tableMetadata.exists()) {
            tableMetadata.getDefinition().getSchema().getFields()
                .forEach(field -> {
                    filedMap.put(field.getName().toUpperCase(), field);
                });
        }
        return filedMap;
    }
}
