package kafka.connect.gcp.bigquery.transform;

/**
 * 
 * @author Sanju Thomas
 *
 */
public interface Transformer<T, R> {

    R transform(T t);
}
