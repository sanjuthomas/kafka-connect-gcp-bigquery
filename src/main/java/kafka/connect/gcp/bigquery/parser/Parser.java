package kafka.connect.gcp.bigquery.parser;

public interface Parser<T, R> {
    
    R parse(T t);

}
