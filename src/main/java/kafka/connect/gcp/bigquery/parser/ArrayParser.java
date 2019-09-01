package kafka.connect.gcp.bigquery.parser;

/**
 *
 * @author Sanju Thomas
 *
 */
public class ArrayParser implements Parser<Object, Object> {

    private String delimiter;

    public ArrayParser(final String delimiter) {
        this.delimiter = delimiter;
    }

	@Override
	public Object parse(final Object t) {
	    if(t instanceof String) {
	        return ((String) t).split(delimiter);
        }
        return t;
	}
}
