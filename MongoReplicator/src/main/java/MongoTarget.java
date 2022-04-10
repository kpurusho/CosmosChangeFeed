
import core.EventHubConsumer;
import writer.MongoWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MongoTarget {

    public static final String mongoUrl = "ToBeFilled";
    public static final String dbname = "flight";
    public static final String collectionname = "status";
    public static final String shardkey = "finalcity";

    public static final String eventEndPoint = "ToBeFilled";
    public static final int eventHubPartitionCount = 2;

    public static void main(String[] args) throws IOException, InterruptedException {
        List<EventHubConsumer> receivers = getReceivers(eventHubPartitionCount);
        List<Thread> writerThreads = new ArrayList<>();
        List<MongoWriter> writers = new ArrayList<>();
        for (EventHubConsumer receiver : receivers) {
            MongoWriter writer = new MongoWriter(mongoUrl, dbname, collectionname, shardkey, receiver);
            writers.add(writer);
            Thread t = new Thread(writer);
            t.start();
            writerThreads.add(t);
        }
        System.out.println("Press q to stop");
        while (true) {
            if (System.in.read() == 'q') break;
        }
        for (MongoWriter writer : writers) {
            writer.stop();
        }
        for (EventHubConsumer receiver : receivers) {
            receiver.close();
        }
        for (Thread t : writerThreads) {
            t.join();
        }
        System.out.println("Done receiving..");
        System.exit(0);
    }

    private static List<EventHubConsumer> getReceivers(int partitionCount) {
        List<EventHubConsumer> receivers = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            receivers.add(new EventHubConsumer(eventEndPoint, String.format("%d", i), "mongoconsumer"));
        }
        return receivers;
    }
}
