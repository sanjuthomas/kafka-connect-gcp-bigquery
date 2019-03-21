package kafka.connect.gcp.bigquery.config;

public class TableConfig {

    private String dataset;
    private String name;
    private String partitionColumn;

    public TableConfig(String dataset, String name, String partitionColumn) {
        this.dataset = dataset;
        this.name = name;
        this.partitionColumn = partitionColumn;
    }

    public boolean isPartitionByDay() {
        return this.partitionColumn == null;
    }
    
    public String dataset() {
        return dataset;
    }
    
    public String name() {
        return name;
    }
    
    public String partitionColumn() {
        return partitionColumn;
    }
}
