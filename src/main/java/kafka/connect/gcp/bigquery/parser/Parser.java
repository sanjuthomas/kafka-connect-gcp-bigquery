package kafka.connect.gcp.bigquery.parser;

/**
 * 
 * @author Sanju Thomas
 *
 */

public interface Parser<T, R> {

	public R parse(T t);

}
