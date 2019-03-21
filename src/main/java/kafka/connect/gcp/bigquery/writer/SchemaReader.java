package kafka.connect.gcp.bigquery.writer;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.TableField;
import com.google.cloud.bigquery.BigQuery.TableOption;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;

import kafka.connect.gcp.bigquery.config.TableConfig;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class SchemaReader {

    public static Map<String, StandardSQLTypeName> tableSchemaFor(BigQuery bigQuery, TableConfig tableConfig) {
        Table tableMetadata = bigQuery.getTable(TableId.of(tableConfig.dataset(), tableConfig.name()), TableOption.fields(TableField.SCHEMA));
        Map<String, StandardSQLTypeName> filedMap = new LinkedHashMap<>();
        if (null != tableMetadata) {
            tableMetadata.getDefinition().getSchema().getFields()
                    .forEach(f -> filedMap.put(f.getName().toUpperCase(), f.getType().getStandardType()));
        }
        return filedMap;
    }
}
