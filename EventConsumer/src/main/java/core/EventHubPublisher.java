package core;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;

import java.util.List;

public class EventHubPublisher implements Publisher {
    private EventHubProducerClient producer;

    public EventHubPublisher(String connectionString, String eventHubName) {
        producer = new EventHubClientBuilder()
                .connectionString(connectionString, eventHubName)
                .buildProducerClient();
    }

    @Override
    public void publish(List<String> data, String partitionKeyValue) {
        CreateBatchOptions options = new CreateBatchOptions();
        EventDataBatch batch = producer.createBatch(options.setPartitionKey(partitionKeyValue));
        for (String d : data) {
            if (!batch.tryAdd(new EventData(d))) {
                //batch is full
                producer.send(batch);
                batch = producer.createBatch(options.setPartitionKey(partitionKeyValue));
            }
        }
        if (batch.getCount() > 0)
            producer.send(batch);
    }

    @Override
    public void close() {
        producer.close();
    }
}
