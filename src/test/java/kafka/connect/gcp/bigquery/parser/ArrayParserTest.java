package kafka.connect.gcp.bigquery.parser;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArrayParserTest {

    private ArrayParser arrayParser;

    @BeforeEach
    public void setup() {
        arrayParser = new ArrayParser(",");
    }

    @Test
    void shouldParseString() {
        Object [] data = (Object[]) arrayParser.parse("1, 2, 3");
        assertArrayEquals(new Object[] {1, 2, 3}, new Object[] {1, 2, 3});
    }
}
