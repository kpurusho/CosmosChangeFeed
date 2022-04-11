package core;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.implementation.NotFoundException;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;

class CheckpointItem {
    public CheckpointItem() {
    }
    public CheckpointItem(String id, String value) {
        this.id = id;
        this.value = value;
    }
    public String id;
    public String value;
}

public class CosmosCheckpointStore implements CheckpointStore {
    public CosmosContainer container;

    public CosmosCheckpointStore(CosmosClient client,
                                 String checkpointDbName,
                                 String checkpointContainerName) {
        client.createDatabaseIfNotExists(checkpointDbName);
        client.getDatabase(checkpointDbName).
                createContainerIfNotExists(checkpointContainerName, "/id");
        container = client.getDatabase(checkpointDbName).getContainer(checkpointContainerName);
    }
    @Override
    public void addCheckpoint(String key, String value) {
        container.upsertItem(new CheckpointItem(key, value));
    }

    @Override
    public String getCheckpoint(String key) {
        try {
            CosmosItemResponse<CheckpointItem> item = container.readItem(key, new PartitionKey(key), CheckpointItem.class);
            if (item.getStatusCode() == 200)
                return item.getItem().value;
        } catch (NotFoundException e) {
            return "";
        }
        return "";
    }
}
