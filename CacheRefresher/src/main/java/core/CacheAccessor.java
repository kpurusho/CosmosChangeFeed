package core;

public interface CacheAccessor {
    void set(String key, String value);
    String get(String key);
}

