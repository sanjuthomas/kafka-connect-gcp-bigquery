package kafka.connect.gcp.bigquery.wirter;

public interface Writer <T, R> {
	
	R write(T t);
}
