package core;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.models.EventPosition;
import reactor.core.Disposable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class EventHubConsumer implements Consumer {
    private final EventHubConsumerAsyncClient consumer;
    private String partitionId;
    private BlockingQueue<String> buffer = new ArrayBlockingQueue<String>(100);
    private Disposable subscription;

    public EventHubConsumer(String connectionString, String partitionId, String consumerGroup) {
        consumer = new EventHubClientBuilder()
                .connectionString(connectionString)
                .consumerGroup(consumerGroup).buildAsyncConsumerClient();
        this.partitionId = partitionId;
        startConsuming();
    }

    private void startConsuming() {
        subscription = consumer.receiveFromPartition(partitionId, EventPosition.latest())
                .subscribe(partitionEvent -> {
                            EventData event = partitionEvent.getData();
                            String content = new String(event.getBody());
                            buffer.add(content);
                        },
                        error -> {
                            System.err.println("Error occurred while consuming events: " + error);
                        }, () -> {
                            System.out.println("Finished reading events.");
                        });
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
        while (true) {
            String e = buffer.poll();
            if (e != null)
                data.add(e);
            else
                break;
        }
        return data;
    }

    @Override
    public void close() {
        subscription.dispose();
        consumer.close();
    }

}
