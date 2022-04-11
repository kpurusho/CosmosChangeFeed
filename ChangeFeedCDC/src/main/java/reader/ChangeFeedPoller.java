package reader;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosChangeFeedRequestOptions;
import com.azure.cosmos.models.FeedRange;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.databind.node.ObjectNode;
import core.CheckpointStore;
import core.Publisher;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class ChangeFeedPoller implements Runnable{
    private final String dbName;
    private final String containerName;
    private final String checkpointKey;
    CosmosContainer container;
    private String partitionKey;
    private String partitionKeyValue = "";
    private CheckpointStore checkpointStore;
    private Publisher publisher;
    private final int batchSize = 100;
    private String lastContinuationToken;
    private long timeEpoch = 0;
    AtomicBoolean stopRequested = new AtomicBoolean(false);

    public ChangeFeedPoller(CosmosClient client, Qualifier q, CheckpointStore checkpointStore,
                            Publisher publisher) {
        this.dbName = q.getDbName();
        this.containerName = q.getContainerName();
        this.partitionKey = q.getPk();
        this.partitionKeyValue = q.getPkValue();
        this.checkpointKey = dbName + "." + containerName + "." + partitionKeyValue;
        this.container = client.getDatabase(dbName).getContainer(containerName);
        this.checkpointStore = checkpointStore;
        this.publisher = publisher;
        this.lastContinuationToken = checkpointStore.getCheckpoint(checkpointKey);
        timeEpoch = System.currentTimeMillis() / 1000;
    }
    public ChangeFeedPoller(CosmosClient client, Qualifier q, CheckpointStore checkpointStore,
                            String continuationToken, Publisher publisher) {
        this(client, q, checkpointStore, publisher);
        this.lastContinuationToken = continuationToken;
    }

    public ChangeFeedPoller(CosmosClient client, Qualifier q, CheckpointStore checkpointStore,
                            long timeEpoch, Publisher publisher) {
        this(client, q, checkpointStore, publisher);
        this.timeEpoch = timeEpoch;
    }

    public void stopPolling() {
        stopRequested.set(true);
    }

    private CosmosChangeFeedRequestOptions createFeedRequestOptions() {
        if (!Objects.equals(lastContinuationToken, "")) {
            //If lastContiunationToken is available then start from token
            return CosmosChangeFeedRequestOptions.
                    createForProcessingFromContinuation(
                            lastContinuationToken).
                    setMaxItemCount(batchSize);
        }
        if (timeEpoch == 0) {
            //Start from beginning
            return CosmosChangeFeedRequestOptions.
                    createForProcessingFromBeginning(getFeedRange()).
                    setMaxItemCount(batchSize);
        }
        //Start from user specified point in time
        return CosmosChangeFeedRequestOptions.
                createForProcessingFromPointInTime(
                        Instant.ofEpochSecond(timeEpoch), getFeedRange()).
                setMaxItemCount(batchSize);
    }

    private FeedRange getFeedRange() {
        if (partitionKeyValue == "")
            return FeedRange.forFullRange();
        else
            return FeedRange.forLogicalPartition(new PartitionKey(partitionKeyValue));
    }

    @Override
    public void run() {
        while (!stopRequested.get()) {
            try {
                //for each page
                for (FeedResponse<ObjectNode> response : container.queryChangeFeed(createFeedRequestOptions(),
                        ObjectNode.class).iterableByPage()) {
                    //save the continuation token
                    lastContinuationToken = response.getContinuationToken();
                    //publish 'changed items' in current page to external message queue
                    if (publishPage(response) == 0)
                        Thread.sleep(100);  //If no 'changed items' available in current page, then wait for 100ms

                    //Persist the continuation token to resume later in case of crash
                    checkpointStore.addCheckpoint(checkpointKey, lastContinuationToken);
                }
            } catch (Exception ex) {
                System.out.println("Failed due to exception:" + ex.getClass().getName() + ", retrying..");
            }
        }
    }

    private int publishPage(FeedResponse<ObjectNode> response) {
        int docPublished = 0;
        List<String> changes = new ArrayList<>();
        String lastPkValue = partitionKeyValue;
        //send 'changed items' batched by partition key value
        for (ObjectNode jsonNodes : response.getResults()) {
            changes.add(jsonNodes.toString());
            lastPkValue = publishItems(changes, lastPkValue, jsonNodes);
            docPublished++;
        }
        publishPendingItems(changes, lastPkValue);

        return docPublished;
    }

    private void publishPendingItems(List<String> changes, String lastPkValue) {
        if (changes.size() > 0)
            publisher.publish(changes, lastPkValue);
    }

    private String publishItems(List<String> changes, String lastPkValue, ObjectNode jsonNodes) {
        String pkValue = jsonNodes.get(partitionKey).toString();
        System.out.println(String.format("%s,%s", jsonNodes.get("id").toString(), pkValue));
        if (lastPkValue != pkValue) {
            publisher.publish(changes, pkValue);
            changes.clear();
            lastPkValue = pkValue;
        }
        return lastPkValue;
    }
}

