package core;

import java.util.List;

public interface Consumer {
    List<String> consume();
    void close();
}

