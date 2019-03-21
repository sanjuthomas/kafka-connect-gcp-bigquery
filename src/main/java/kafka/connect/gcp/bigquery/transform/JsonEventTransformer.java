package kafka.connect.gcp.bigquery.transform;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.StandardSQLTypeName;

import kafka.connect.gcp.bigquery.parser.Parser;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class JsonEventTransformer implements Transformer<SinkRecord, Map<String, ? extends Object>> {

    private Map<String, StandardSQLTypeName> fieldTypeMap;

    private final TypeReference<Map<String, Object>> typeReference = new TypeReference<Map<String, Object>>() {};
    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private Map<StandardSQLTypeName, Parser> parsers = new HashMap<>();

    public JsonEventTransformer(Map<String, StandardSQLTypeName> fieldTypeMap) {
        this.fieldTypeMap = fieldTypeMap;
    }

    @Override
    public Map<String, ? extends Object> transform(SinkRecord record) {

        final Map<String, Object> payload = OBJECT_MAPPER.convertValue(record.value(), new TypeReference<Map<String, Object>>() {});
        return null;
    }
    
    private Object toDatabaseType(final Map<String, Object> payload) {
        
        for(String key : payload.keySet()) {
            StandardSQLTypeName sqlType = fieldTypeMap.get(key);
            if (sqlType != null) {
                switch (sqlType) {
                case DATE:
                    return parsers.get(sqlType).parse(payload.get(key));
                case STRUCT:
                    return parsers.get(sqlType).parse(payload.get(key));
                case TIMESTAMP:
                    return parsers.get(sqlType).parse(payload.get(key));
                case BOOL:
                    return parsers.get(sqlType).parse(payload.get(key));
                // no known translation at this point for the rest
                case ARRAY:
                case BYTES:
                case DATETIME:
                case FLOAT64:
                case INT64:
                case STRING:
                case TIME:
                default:
                    return payload.get(key);
                }
            }
        }
        return null;
    }

}
