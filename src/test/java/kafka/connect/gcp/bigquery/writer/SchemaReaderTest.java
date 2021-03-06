package kafka.connect.gcp.bigquery.writer;

import static org.junit.jupiter.api.Assertions.*;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.StandardSQLTypeName;
import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import kafka.connect.gcp.bigquery.config.TableConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchemaReaderTest {

    private SchemaReader schemaReader;

    @BeforeEach
    void setUp() throws Exception {
        GoogleCredentials credentials;
        final File credentialsPath = new File(
            "src/test/resource/civic-athlete-251623-9cf9141366a2.json");
        try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
            credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
        }
        schemaReader = new SchemaReader(
            BigQueryOptions.newBuilder().setCredentials(credentials).build().getService());
    }

    @Test
    void shouldGetSchemaForGivenTable() {
        final Map<String, Field> schema = schemaReader
            .tableSchemaFor(new TableConfig("kafkadataset", "QuoteRequest"));

        assertEquals(StandardSQLTypeName.STRING,
            schema.get("ACCOUNT_ID").getType().getStandardType());
        assertEquals(StandardSQLTypeName.STRING,
            schema.get("CLIENT_ID").getType().getStandardType());
        assertEquals(StandardSQLTypeName.STRING, schema.get("SYMBOL").getType().getStandardType());
        assertEquals(StandardSQLTypeName.INT64, schema.get("QUANTITY").getType().getStandardType());
        assertEquals(StandardSQLTypeName.STRING, schema.get("TOPIC").getType().getStandardType());
        assertEquals(StandardSQLTypeName.INT64,
            schema.get("PARTITION").getType().getStandardType());
        assertEquals(StandardSQLTypeName.INT64, schema.get("OFFSET").getType().getStandardType());

        assertEquals(Mode.NULLABLE, schema.get("OFFSET").getMode());
        assertEquals(Mode.NULLABLE, schema.get("CLIENT_ID").getMode());
        assertEquals(Mode.NULLABLE, schema.get("SYMBOL").getMode());
        assertEquals(Mode.NULLABLE, schema.get("QUANTITY").getMode());
        assertEquals(Mode.NULLABLE, schema.get("TOPIC").getMode());
        assertEquals(Mode.NULLABLE, schema.get("PARTITION").getMode());
        assertEquals(Mode.NULLABLE, schema.get("OFFSET").getMode());
    }
}
