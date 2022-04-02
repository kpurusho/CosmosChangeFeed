package core;

import com.azure.core.util.IterableStream;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class EventHubConsumer implements Consumer {
    private final EventHubConsumerClient consumer;
    private String partitionId;
    private Long lastSeqNumber = Long.valueOf(-1);

    public EventHubConsumer(String connectionString, String partitionId) {
        consumer = new EventHubClientBuilder()
                .connectionString(connectionString)
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME).buildConsumerClient();
        this.partitionId = partitionId;
    }

    @Override
    public List<String> consume() {
        try {
            return getData();
        }
        catch (Exception ex) {
            return getData();
        }
    }

    private List<String> getData() {
        List<String> data = new ArrayList<>();
        IterableStream<PartitionEvent> result = consumer.receiveFromPartition(partitionId,
                100,
                getEventPosition(),
                Duration.ofSeconds(1));
        for(PartitionEvent e : result) {
            data.add(new String(e.getData().getBody()));
            lastSeqNumber = e.getData().getSequenceNumber();
        }
        return data;
    }

    @Override
    public void close() {
        consumer.close();
    }

    private EventPosition getEventPosition() {
        if (lastSeqNumber == -1)
            return EventPosition.latest();
        return EventPosition.fromSequenceNumber(lastSeqNumber);
    }
}
