package core;

import java.util.List;

public interface Publisher {
    void publish(List<String> data, String partitionKeyValue);
    void close();
}

