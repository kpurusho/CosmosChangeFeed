package core;

public interface CheckpointStore {
    void addCheckpoint(String key, String value);

    String getCheckpoint(String key);
}

