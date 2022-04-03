import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.ThrottlingRetryOptions;
import core.CosmosCheckpointStore;
import core.EventHubPublisher;
import core.Publisher;
import reader.ChangeFeedPoller;
import reader.Qualifier;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class SourceCDC {

    public static final String cosmosEndPoint = "https://kartcosmossql.documents.azure.com:443/";
    public static final String cosmosKey = "XFmJKi0cquhDZ47QQHXmdRuWrIR9RzPqXGwRFvhJmHaEwmSrXaFW4BSHA48OLCfJ90hHeoExBNPQaOY6Q5dBag==";
    public static final String dbName = "flight";
    public static final String containerName = "schedule";
    public static final String partitionKey = "finalcity";
    public static final String[] partitionKeyValues = {"Chennai", "Mumbai"};

    public static final String eventEndPoint = "Endpoint=sb://kartevent.servicebus.windows.net/;SharedAccessKeyName=eventpolicy;SharedAccessKey=xtyonwvbR7SItuFlbHzkk0xkcvvblwSV3UwpTgXYfP8=;EntityPath=employeehub";
    public static final String eventHub = "employeehub";

    public static void main(String[] args) throws IOException, InterruptedException {
        ThrottlingRetryOptions retryOptions = new ThrottlingRetryOptions();
        retryOptions.setMaxRetryWaitTime(Duration.ofSeconds(60));
        retryOptions.setMaxRetryAttemptsOnThrottledRequests(20);
        CosmosClient cosmosClient = new CosmosClientBuilder()
                .endpoint(cosmosEndPoint)
                .key(cosmosKey)
                .throttlingRetryOptions(retryOptions)
                .buildClient();
        Publisher publisher = new EventHubPublisher(eventEndPoint, eventHub);
        CosmosCheckpointStore store = new CosmosCheckpointStore(cosmosClient);
        List<ChangeFeedPoller> pollers = new ArrayList<>();
        List<Thread> pollerThreads = new ArrayList<>();
        for (String partitionKeyValue : partitionKeyValues) {
            Qualifier q = new Qualifier(dbName, containerName, partitionKey, partitionKeyValue);
            ChangeFeedPoller poller = new ChangeFeedPoller(cosmosClient, q, store, 0, publisher);
            Thread t = new Thread(poller);
            pollers.add(poller);
            pollerThreads.add(t);
            t.start();
        }
        System.out.println("Press q to stop polling..");
        while (true) {
            if (System.in.read() == 'q') break;
        }
        for (ChangeFeedPoller poller : pollers) {
            poller.stopPolling();
        }
        for (Thread t : pollerThreads) {
            t.join();
        }
        publisher.close();
        System.out.println("Done polling..");
        System.exit(0);
    }
}
