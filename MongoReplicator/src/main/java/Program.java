

import core.EventHubConsumer;
import writer.MongoWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Program {

    public static final String mongoUrl = "mongodb://kartmongo:YQXIxujXU7H6zgnT26Yq5vY6eHNUDcFZZbBJd7WJIDokOhj3DpipoEDBdkSJ4SoVBQP1ibySUN27m5dKrWSnPA==@kartmongo.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@kartmongo@";
    public static final String dbname = "flight";
    public static final String collectionname = "status";
    public static final String shardkey = "finalcity";

    public static final String eventEndPoint = "Endpoint=sb://kartevent.servicebus.windows.net/;SharedAccessKeyName=eventpolicy;SharedAccessKey=xtyonwvbR7SItuFlbHzkk0xkcvvblwSV3UwpTgXYfP8=;EntityPath=employeehub";
    public static final String eventHub = "employeehub";
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
        System.out.println("Press any key to stop");
        System.in.read();
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
            receivers.add(new EventHubConsumer(eventEndPoint, String.format("%d", i)));
        }
        return receivers;
    }
}
