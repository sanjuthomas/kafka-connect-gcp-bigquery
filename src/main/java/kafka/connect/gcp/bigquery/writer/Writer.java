package kafka.connect.gcp.bigquery.writer;

public interface Writer <T, R> {
	
	R write(T t);
}
