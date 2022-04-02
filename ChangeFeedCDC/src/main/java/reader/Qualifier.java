package reader;

public class Qualifier {
    private String dbName;
    private String containerName;
    private String pk;
    private String pkValue;

    public Qualifier(String dbName, String containerName, String pk, String pkValue) {
        this.dbName = dbName;
        this.containerName = containerName;
        this.pk = pk;
        this.pkValue = pkValue;
    }

    public String getDbName() {
        return dbName;
    }

    public String getContainerName() {
        return containerName;
    }

    public String getPk() {
        return pk;
    }

    public String getPkValue() {
        return pkValue;
    }
}
